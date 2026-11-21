// Diagnostic harness: prints facts about the scoring machine and stage timings to stderr.
// It is submitted on purpose as a failing entry (the dev check returns the logs of a failed run),
// and is never the team's scored entry.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/uio.h>
#include <cerrno>
#include <map>
#include <string_view>
#include <stdexcept>
#include <cstdint>
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
using namespace std;
namespace fs=std::filesystem;
static double now(){return chrono::duration<double>(chrono::steady_clock::now().time_since_epoch()).count();}
static string slurp(const string& p,size_t maxb=4096){ifstream in(p,ios::binary);if(!in)return "<none>";string s(maxb,'\0');in.read(s.data(),maxb);s.resize(in.gcount());while(!s.empty()&&(s.back()=='\n'||s.back()==' '))s.pop_back();return s;}
static void show(const char* label,const string& path){fprintf(stderr,"DIAG %s: %s\n",label,slurp(path,300).c_str());}
template<class F> static double par(int T,F&& f){double t=now();vector<thread> th;for(int i=0;i<T;++i)th.emplace_back([&,i]{f(i);});for(auto& x:th)x.join();return now()-t;}
// ---- Minimal Parquet writer: PLAIN encoding, no compression, one data page per column chunk. ----
enum{PQ_INT32=1,PQ_INT64=2,PQ_BYTES=6};
enum{CONV_NONE=-1,CONV_UTF8=0,CONV_DATE=6};
struct PqCol{const char* name;int type;bool optional;int conv;};
static void put_varint(string& o,uint64_t v){while(v>=0x80){o.push_back((char)(v|0x80));v>>=7;}o.push_back((char)v);}
static void put_zz(string& o,int64_t v){put_varint(o,((uint64_t)v<<1)^(uint64_t)(v>>63));}
static void put_le32(string& o,uint32_t v){o.append((const char*)&v,4);}
// Thrift compact protocol, just enough for the Parquet footer and page headers.
struct Thrift{
 string& o;vector<int> stack;int last=0;
 explicit Thrift(string& out):o(out){}
 void field(int id,int type){int d=id-last;if(d>0&&d<=15)o.push_back((char)((d<<4)|type));else{o.push_back((char)type);put_zz(o,id);}last=id;}
 void i32(int id,int64_t v){field(id,5);put_zz(o,v);}
 void i64(int id,int64_t v){field(id,6);put_zz(o,v);}
 void str(int id,string_view s){field(id,8);put_varint(o,s.size());o.append(s);}
 void list(int id,int elem,size_t n){field(id,9);if(n<15)o.push_back((char)((n<<4)|elem));else{o.push_back((char)(0xF0|elem));put_varint(o,n);}}
 void open_struct(int id){field(id,12);stack.push_back(last);last=0;}
 void open_elem(){stack.push_back(last);last=0;}
 void close(){o.push_back(0);last=stack.back();stack.pop_back();}
};
struct PqFile{
 struct Chunk{int64_t offset,size,nvalues;};
 struct Group{int64_t rows,bytes;vector<Chunk> cols;};
 int fd=-1;int64_t pos=0,rows=0;vector<PqCol> cols;vector<Group> groups;string buf;
 bool is_open()const{return fd>=0;}
 void write_all(const char* p,size_t n){
  while(n){ssize_t w=::write(fd,p,n);if(w<0){if(errno==EINTR)continue;throw runtime_error("Parquet write failed");}p+=w;n-=(size_t)w;}
 }
 void open(const fs::path& path,vector<PqCol> c){
  cols=std::move(c);fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);
  if(fd<0)throw runtime_error("Cannot create "+path.string());
  write_all("PAR1",4);pos=4;
 }
 // bodies[i] is the finished page body of column i: optional definition levels, then PLAIN values.
 // The bodies are written from where they are (writev); only the small page headers are built here.
 void row_group(int64_t nrows,const vector<string_view>& bodies){
  buf.clear();Group g;g.rows=nrows;g.bytes=0;
  const size_t nc=cols.size();vector<size_t> hoff(nc+1);int64_t at=pos;
  for(size_t i=0;i<nc;++i){
   hoff[i]=buf.size();
   if(bodies[i].size()>0x7fff0000u)throw runtime_error("Parquet page too large");
   Thrift h(buf);
   h.i32(1,0);h.i32(2,(int64_t)bodies[i].size());h.i32(3,(int64_t)bodies[i].size());
   h.open_struct(5);h.i32(1,nrows);h.i32(2,0);h.i32(3,3);h.i32(4,3);h.close();
   buf.push_back(0);
   const int64_t size=(int64_t)(buf.size()-hoff[i]+bodies[i].size());
   g.cols.push_back({at,size,nrows});g.bytes+=size;at+=size;
  }
  hoff[nc]=buf.size();
  vector<struct iovec> iov;iov.reserve(2*nc);
  for(size_t i=0;i<nc;++i){
   iov.push_back({(void*)(buf.data()+hoff[i]),hoff[i+1]-hoff[i]});
   if(!bodies[i].empty())iov.push_back({(void*)bodies[i].data(),bodies[i].size()});
  }
  size_t k=0;
  while(k<iov.size()){
   ssize_t w=::writev(fd,iov.data()+k,(int)min<size_t>(iov.size()-k,64));
   if(w<0){if(errno==EINTR)continue;throw runtime_error("Parquet write failed");}
   size_t left=(size_t)w;
   while(k<iov.size()&&left>=iov[k].iov_len){left-=iov[k].iov_len;++k;}
   if(left){iov[k].iov_base=(char*)iov[k].iov_base+left;iov[k].iov_len-=left;}
  }
  pos=at;rows+=nrows;groups.push_back(std::move(g));
 }
 void close(){
  buf.clear();Thrift m(buf);
  m.i32(1,1);
  m.list(2,12,cols.size()+1);
  m.open_elem();m.str(4,"schema");m.i32(5,(int64_t)cols.size());m.close();
  for(const auto& c:cols){
   m.open_elem();m.i32(1,c.type);m.i32(3,c.optional?1:0);m.str(4,c.name);
   if(c.conv!=CONV_NONE){m.i32(6,c.conv);m.open_struct(10);m.open_struct(c.conv==CONV_UTF8?1:6);m.close();m.close();}
   m.close();
  }
  m.i64(3,rows);
  m.list(4,12,groups.size());
  for(const auto& g:groups){
   m.open_elem();
   m.list(1,12,cols.size());
   for(size_t i=0;i<cols.size();++i){const auto& ch=g.cols[i];
    m.open_elem();m.i64(2,ch.offset);
    m.open_struct(3);
    m.i32(1,cols[i].type);
    m.list(2,5,2);put_zz(buf,0);put_zz(buf,3);
    m.list(3,8,1);put_varint(buf,strlen(cols[i].name));buf.append(cols[i].name);
    m.i32(4,0);m.i64(5,ch.nvalues);m.i64(6,ch.size);m.i64(7,ch.size);m.i64(9,ch.offset);
    m.close();
    m.close();
   }
   m.i64(2,g.bytes);m.i64(3,g.rows);
   m.close();
  }
  m.str(6,"shopping-core");
  buf.push_back(0);
  put_le32(buf,(uint32_t)buf.size());buf.append("PAR1",4);
  write_all(buf.data(),buf.size());
  if(::close(fd)!=0)throw runtime_error("Parquet close failed");
  fd=-1;
 }
};
static void put_bytes(string& o,string_view s){put_le32(o,(uint32_t)s.size());o.append(s);}
template<class V> static string_view raw(const V& v){return string_view((const char*)v.data(),v.size()*sizeof(v[0]));}

struct RunResult{string label;double wall=0;int rc=0;map<string,double> t;};
static vector<RunResult> results;
// Runs the pipeline as a child, forwards its stderr to ours and keeps its "TIME <stage> <seconds>" lines.
static int run_child(const string& bin,vector<pair<string,string>> env,double& wall,map<string,double>& marks){
 int pfd[2];if(pipe(pfd)!=0)return -2;
 double t=now();pid_t pid=fork();
 if(pid==0){dup2(pfd[1],2);close(pfd[0]);close(pfd[1]);for(auto& kv:env)setenv(kv.first.c_str(),kv.second.c_str(),1);execl(bin.c_str(),bin.c_str(),(char*)nullptr);_exit(127);}
 close(pfd[1]);string out;char buf[4096];ssize_t n;
 while((n=read(pfd[0],buf,sizeof buf))>0)out.append(buf,n);
 close(pfd[0]);
 int st=0;waitpid(pid,&st,0);wall=now()-t;
 fwrite(out.data(),1,out.size(),stderr);
 size_t p=0;while(p<out.size()){size_t e=out.find('\n',p);if(e==string::npos)e=out.size();string line=out.substr(p,e-p);p=e+1;
  if(line.rfind("TIME ",0)==0){size_t sp=line.find(' ',5);if(sp!=string::npos)marks[line.substr(5,sp-5)]+=atof(line.c_str()+sp+1);}}
 return WIFEXITED(st)?WEXITSTATUS(st):-1;
}
// Writes `reps` copies of a CSV directory with a replica letter put in front of the given ID fields,
// so every copy has its own events and people. Returns bytes written.
static uint64_t replicate(const fs::path& src,const fs::path& dst,int reps,int idFields){
 fs::create_directories(dst);uint64_t total=0;int part=0;
 vector<fs::path> files;for(auto& f:fs::directory_iterator(src))if(f.path().extension()==".csv")files.push_back(f.path());
 sort(files.begin(),files.end());
 atomic<size_t> next{0};atomic<uint64_t> bytes{0};
 struct Job{fs::path in;int rep;int part;};vector<Job> jobs;
 for(int r=0;r<reps;++r)for(auto& f:files)jobs.push_back({f,r,part++});
 par(8,[&](int){for(;;){size_t j=next.fetch_add(1);if(j>=jobs.size())break;const Job& jb=jobs[j];
   string in;{ifstream f(jb.in,ios::binary);in.assign(istreambuf_iterator<char>(f),{});}
   string out;out.reserve(in.size()+in.size()/8);
   size_t p=in.find('\n');if(p==string::npos){p=in.size();}else ++p;
   out.append(in,0,p); // header
   const char tag=(char)('A'+jb.rep);
   while(p<in.size()){size_t e=in.find('\n',p);if(e==string::npos)e=in.size();
    size_t q=p;
    for(int k=0;k<idFields&&q<=e;++k){ // tag the start of field k unless it is empty
     size_t c=in.find(',',q);if(c==string::npos||c>e)c=e;
     if(c>q){if(in[q]=='"'){out.push_back('"');if(c-q>2)out.push_back(tag);out.append(in,q+1,c-q-1);}else{out.push_back(tag);out.append(in,q,c-q);}}
     if(c<e)out.push_back(',');
     q=c+1;
    }
    if(q<=e)out.append(in,q,e-q);
    out.push_back('\n');p=e+1;
   }
   char name[64];snprintf(name,sizeof name,"part-%05d.csv",jb.part);
   FILE* f=fopen((dst/name).c_str(),"wb");if(f){fwrite(out.data(),1,out.size(),f);fclose(f);}
   bytes+=out.size();
 }});
 total=bytes.load();return total;
}
static double gMemsetGBs=0,gTouchUsPage=0,gPopulateRc=-9,gTouch8[4]={0,0,0,0},gSpin[3]={0,0,0},gReadGBs[2]={0,0},gReplicaSec=0,gMunmap[4]={0,0,0,0};
static string gCpu,gKernel,gThp,gDefrag,gFs;static int gAff=0,gFlags=0;
int main(){
 setvbuf(stderr,nullptr,_IOLBF,0);
 const double T0=now();
 const string data=getenv("DATA_DIR")?getenv("DATA_DIR"):"/data",outdir=getenv("OUTPUT_DIR")?getenv("OUTPUT_DIR"):"/output",scratch=getenv("SCRATCH_DIR")?getenv("SCRATCH_DIR"):"/scratch";
 const string core=getenv("SS_CORE")?getenv("SS_CORE"):"/app/shopping-core";
 // ---- machine facts ----
 utsname u{};uname(&u);gKernel=u.release;fprintf(stderr,"DIAG kernel: %s %s %s\n",u.sysname,u.release,u.machine);
 cpu_set_t cs;CPU_ZERO(&cs);sched_getaffinity(0,sizeof cs,&cs);gAff=CPU_COUNT(&cs);
 string cpus;for(int i=0;i<CPU_SETSIZE;++i)if(CPU_ISSET(i,&cs))cpus+=to_string(i)+" ";
 fprintf(stderr,"DIAG nproc_conf=%ld nproc_onln=%ld affinity_count=%d cpus=[%s] NCPU=%s MEM_GB=%s\n",sysconf(_SC_NPROCESSORS_CONF),sysconf(_SC_NPROCESSORS_ONLN),CPU_COUNT(&cs),cpus.c_str(),getenv("NCPU")?getenv("NCPU"):"-",getenv("MEM_GB")?getenv("MEM_GB"):"-");
 {ifstream ci("/proc/cpuinfo");string line;int shown=0;string flags;
  while(getline(ci,line)){if(shown<1&&(line.rfind("model name",0)==0||line.rfind("cpu MHz",0)==0||line.rfind("cache size",0)==0||line.rfind("siblings",0)==0||line.rfind("cpu cores",0)==0||line.rfind("microcode",0)==0)){fprintf(stderr,"DIAG cpuinfo %s\n",line.c_str());}
   if(line.rfind("flags",0)==0&&flags.empty())flags=line;if(line.empty())++shown;}
  string keep;for(const char* f:{"sse4_2","avx2","bmi2","avx512f","avx512bw","avx512vl","avx512vbmi","avx512_vbmi2","gfni","vaes","vpclmulqdq","erms","fsrm","pdpe1gb","hypervisor","clwb","amx_tile","avx_vnni"})if(flags.find(string(" ")+f+" ")!=string::npos||flags.rfind(string(" ")+f)==flags.size()-strlen(f)-1)keep+=string(f)+" ";
  fprintf(stderr,"DIAG cpu flags: %s\n",keep.c_str());
  int bit=0;for(const char* f:{"avx2","bmi2","avx512f","avx512bw","avx512vl","avx512vbmi","avx512_vbmi2","erms","fsrm","hypervisor"}){if(keep.find(string(f)+" ")!=string::npos)gFlags|=1<<bit;++bit;}
  ifstream c2("/proc/cpuinfo");string l2;while(getline(c2,l2))if(l2.rfind("model name",0)==0){gCpu=l2.substr(l2.find(':')+2);break;}}
 gThp=slurp("/sys/kernel/mm/transparent_hugepage/enabled",80);gDefrag=slurp("/sys/kernel/mm/transparent_hugepage/defrag",80);
 for(int i=0;i<CPU_SETSIZE;++i)if(CPU_ISSET(i,&cs)){string b="/sys/devices/system/cpu/cpu"+to_string(i);
  fprintf(stderr,"DIAG cpu%d core_id=%s siblings=%s maxfreq=%s L2=%s L3=%s\n",i,slurp(b+"/topology/core_id",16).c_str(),slurp(b+"/topology/thread_siblings_list",32).c_str(),slurp(b+"/cpufreq/cpuinfo_max_freq",16).c_str(),slurp(b+"/cache/index2/size",16).c_str(),slurp(b+"/cache/index3/size",16).c_str());}
 show("thp enabled","/sys/kernel/mm/transparent_hugepage/enabled");show("thp defrag","/sys/kernel/mm/transparent_hugepage/defrag");show("thp khugepaged defrag","/sys/kernel/mm/transparent_hugepage/khugepaged/defrag");
 show("cgroup memory.max","/sys/fs/cgroup/memory.max");show("cgroup memory.high","/sys/fs/cgroup/memory.high");show("cgroup cpu.max","/sys/fs/cgroup/cpu.max");show("cgroup cpuset","/sys/fs/cgroup/cpuset.cpus.effective");show("cgroup v1 mem limit","/sys/fs/cgroup/memory/memory.limit_in_bytes");
 show("overcommit","/proc/sys/vm/overcommit_memory");show("fault_around","/sys/kernel/mm/transparent_hugepage/hpage_pmd_size");
 {ifstream mi("/proc/meminfo");string line;while(getline(mi,line))for(const char* k:{"MemTotal","MemFree","MemAvailable","Cached:","AnonHugePages","HugePages_Total","Hugepagesize"})if(line.rfind(k,0)==0)fprintf(stderr,"DIAG meminfo %s\n",line.c_str());}
 {ifstream mi("/proc/self/mountinfo");string line;while(getline(mi,line)){size_t d=line.find(" - ");string mp;{size_t a=0;for(int k=0;k<4;++k)a=line.find(' ',a)+1;mp=line.substr(a,line.find(' ',a)-a);}
   if(mp==data||mp==scratch)gFs+=(d==string::npos?string("?"):line.substr(d+3,line.find(' ',d+3)-(d+3)))+"/";
   if(mp=="/"||mp==data||mp==outdir||mp==scratch||mp=="/tmp")fprintf(stderr,"DIAG mount %s -> %s\n",mp.c_str(),d==string::npos?"?":line.substr(d+3,90).c_str());}}
 {uint64_t b=0;int n=0;for(const char* sub:{"events","clicks","links"})for(auto& f:fs::directory_iterator(fs::path(data)/sub)){b+=fs::file_size(f);++n;}fprintf(stderr,"DIAG data: %d files %.1f MB\n",n,b/1e6);}
 // ---- micro benchmarks (8 threads unless noted) ----
 const size_t CH=size_t(192)<<20;
 for(int mode=0;mode<4;++mode){ // 0 plain faults, 1 huge pages, 2 populate, 3 huge+populate
  vector<char*> mem(8);vector<int> rc(8,0);
  double setup=par(8,[&](int i){mem[i]=(char*)mmap(nullptr,CH,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
    if(mode&1)madvise(mem[i],CH,MADV_HUGEPAGE);if(mode&2)rc[i]=madvise(mem[i],CH,MADV_POPULATE_WRITE);});
  double touch=par(8,[&](int i){for(size_t k=0;k<CH;k+=4096)mem[i][k]=1;});
  double w2=par(8,[&](int i){memset(mem[i],2,CH);});
  double cp=par(4,[&](int i){memcpy(mem[i],mem[i+4],CH);});
  string ah;{ifstream sm("/proc/self/smaps_rollup");string line;while(getline(sm,line))if(line.rfind("AnonHugePages",0)==0)ah=line.substr(14);}
  double un=par(8,[&](int i){munmap(mem[i],CH);});
  gTouch8[mode]=setup+touch;gMunmap[mode]=un;if(mode==0)gMemsetGBs=8*CH/1e9/w2;if(mode==2)gPopulateRc=rc[0];
  fprintf(stderr,"DIAG mem mode=%d (1.5GB, 8 thr): setup %.3fs populate_rc=%d first-touch %.3fs | memset %.3fs (%.1f GB/s) | memcpy 4thr %.3fs (%.1f GB/s) | munmap %.3fs | AnonHuge=%s\n",mode,setup,rc[0],touch,w2,8*CH/1e9/w2,cp,4*CH/1e9/cp,un,ah.c_str());
 }
 {char* m=(char*)mmap(nullptr,CH,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);double t=now();for(size_t k=0;k<CH;k+=4096)m[k]=1;double a=now()-t;t=now();memset(m,3,CH);double b=now()-t;munmap(m,CH);
  gTouchUsPage=a/(CH/4096)*1e6;
  fprintf(stderr,"DIAG mem 1 thread 192MB: first-touch %.3fs (%.2f us/page) memset %.3fs (%.1f GB/s)\n",a,a/(CH/4096)*1e6,b,CH/1e9/b);}
 // spin benchmark: does 8-thread integer work scale? (hyperthread check)
 {auto spin=[](int){volatile uint64_t x=1;for(uint64_t i=0;i<400000000ULL;++i)x=x*6364136223846793005ULL+1442695040888963407ULL;};
  double t1=par(1,spin),t4=par(4,spin),t8=par(8,spin);gSpin[0]=t1;gSpin[1]=t4;gSpin[2]=t8;fprintf(stderr,"DIAG spin: 1thr %.3fs 4thr %.3fs 8thr %.3fs (ideal = equal)\n",t1,t4,t8);}
 // file reading through mmap: first pass (maybe cold), second pass (warm)
 {vector<fs::path> files;for(auto& f:fs::directory_iterator(fs::path(data)/"events"))files.push_back(f.path());
  for(int pass=0;pass<2;++pass){atomic<size_t> next{0};atomic<uint64_t> bytes{0},sum{0};
   double t=par(8,[&](int){for(;;){size_t j=next.fetch_add(1);if(j>=files.size())break;int fd=open(files[j].c_str(),O_RDONLY);struct stat st;fstat(fd,&st);if(st.st_size){
     const char* b=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0);uint64_t s=0;for(off_t k=0;k<st.st_size;k+=64)s+=(unsigned char)b[k];sum+=s;bytes+=st.st_size;munmap((void*)b,st.st_size);}close(fd);}});
   gReadGBs[pass]=bytes/1e9/t;
   fprintf(stderr,"DIAG read /data events pass %d: %.3fs %.1f MB (%.2f GB/s)\n",pass,t,bytes/1e6,bytes/1e9/t);}}
 // ---- pipeline on the dev data, several settings ----
 auto pipeline=[&](const string& label,const string& d,const string& o,vector<pair<string,string>> env){
  env.push_back({"DATA_DIR",d});env.push_back({"OUTPUT_DIR",o});env.push_back({"SCRATCH_DIR",scratch});
  fprintf(stderr,"DIAG RUN %s begin\n",label.c_str());RunResult r;r.label=label;r.rc=run_child(core,env,r.wall,r.t);
  fprintf(stderr,"DIAG RUN %s end: wall %.3fs rc=%d (total elapsed %.1fs)\n",label.c_str(),r.wall,r.rc,now()-T0);results.push_back(r);};
 int runid=0;auto od=[&]{return scratch+"/dout"+to_string(runid++);};
 pipeline("dev n8 a",data,od(),{{"NCPU","8"}});
 pipeline("dev n8 b",data,od(),{{"NCPU","8"}});
 // ---- replicas of the dev data: every copy gets its own events and people ----
 // "huge" has HUGE copies (about the size of the final data); "big" is the first BIG of them (practice size).
 const int HUGE=getenv("SS_DIAG_HUGE")?atoi(getenv("SS_DIAG_HUGE")):60,BIG=getenv("SS_DIAG_BIG")?atoi(getenv("SS_DIAG_BIG")):20;
 const fs::path huge=fs::path(scratch)/"huge",big=fs::path(scratch)/"big";
 {double t=now();uint64_t b=0;
  for(const char* sub:{"events","clicks","links"}){
   b+=replicate(fs::path(data)/sub,huge/sub,HUGE,2);
   size_t nfiles=0;for(auto& f:fs::directory_iterator(fs::path(data)/sub))if(f.path().extension()==".csv")++nfiles;
   fs::create_directories(big/sub);
   for(size_t k=0;k<nfiles*(size_t)BIG;++k){char name[64];snprintf(name,sizeof name,"part-%05zu.csv",k);error_code ec;fs::create_symlink(huge/sub/name,big/sub/name,ec);}
  }
  for(const char* f:{"products.csv","campaigns.csv","manifest.json"}){fs::copy_file(fs::path(data)/f,big/f,fs::copy_options::overwrite_existing);fs::copy_file(fs::path(data)/f,huge/f,fs::copy_options::overwrite_existing);}
  gReplicaSec=now()-t;
  fprintf(stderr,"DIAG replica: %d copies, %.1f MB written in %.2fs (elapsed %.1fs); big = first %d copies\n",HUGE,b/1e6,now()-t,now()-T0,BIG);}
 const string B=big.string(),H=huge.string();
 auto timeleft=[&](double need){if(now()-T0+need>150){fprintf(stderr,"DIAG skipping the rest of this group: out of time (%.1fs elapsed)\n",now()-T0);return false;}return true;};
 pipeline("big n8 default #1",B,od(),{{"NCPU","8"}});
 pipeline("big n8 default #2",B,od(),{{"NCPU","8"}});
 pipeline("big n8 default #3",B,od(),{{"NCPU","8"}});
 if(timeleft(25)){pipeline("huge n8 default #1",H,od(),{{"NCPU","8"}});pipeline("huge n8 default #2",H,od(),{{"NCPU","8"}});}
 struct Var{const char* label;vector<pair<string,string>> env;};
 const vector<Var> vars={
  {"big n8 sse2",{{"NCPU","8"},{"SS_ISA","sse2"}}},
  {"big n8 noprefault",{{"NCPU","8"},{"SS_NOPREFAULT","1"}}},
  {"big n8 nohuge",{{"NCPU","8"},{"SS_NOHUGE","1"}}},
  {"big n8 nohuge noprefault",{{"NCPU","8"},{"SS_NOHUGE","1"},{"SS_NOPREFAULT","1"}}},
  {"big n8 noasyncfree",{{"NCPU","8"},{"SS_NOASYNCFREE","1"}}},
  {"big n4",{{"NCPU","4"}}},{"big n2",{{"NCPU","2"}}},{"big n1",{{"NCPU","1"}}},{"big n16",{{"NCPU","16"}}},
  {"big n8 default #4",{{"NCPU","8"}}}};
 for(const Var& v:vars){if(!timeleft(12))break;pipeline(v.label,B,od(),v.env);}
 // ---- the real output, then break it on purpose so this entry is rejected and the log is returned ----
 pipeline("final /data -> /output",data,outdir,{{"NCPU","8"}});
 // Put the findings into extra campaign_daily rows: the dev check reports the rows that differ.
 // Row order (by campaign_id) is importance order. Integers: sessions = wall time in microseconds,
 // purchasing_sessions = PARSE us, revenue_cents = DEDUP us * 10^8 + AGGREGATE us, assisted_sessions = links us.
 {
  auto us=[](double sec){return (int64_t)(sec*1e6+0.5);};
  auto best=[&](const string& prefix)->RunResult{RunResult b;b.wall=1e9;for(auto& r:results)if(r.label.rfind(prefix,0)==0&&r.rc==0&&r.wall<b.wall)b=r;return b;};
  auto code=[](const string& sset)->int{size_t l=sset.find('[');if(l==string::npos)return 9;string v=sset.substr(l+1,sset.find(']')-l-1);return v=="never"?0:v=="madvise"?1:v=="always"?2:v=="defer"?3:v=="defer+madvise"?4:8;};
  string cid,chan;vector<int32_t> days;vector<int64_t> c1,c2,c3,c4;int idx=0;
  auto row=[&](const string& label,const string& text,int64_t a,int64_t b,int64_t c,int64_t d){
   char id[64];snprintf(id,sizeof id,"!%02d %s",idx++,label.c_str());put_bytes(cid,id);put_bytes(chan,text);days.push_back(20000+idx);
   c1.push_back(a);c2.push_back(b);c3.push_back(c);c4.push_back(d);};
  auto run_row=[&](const string& label,const RunResult& r){
   auto g=[&](const char* k){auto it=r.t.find(k);return it==r.t.end()?0.0:it->second;};
   char text[96];snprintf(text,sizeof text,"L%.0f P%.0f D%.0f A%.0f ms rc%d",(g("MAP")+g("LINKREAD")+g("FLATTEN")+g("LINKS"))*1e3,g("PARSE")*1e3,g("DEDUP")*1e3,g("AGGREGATE")*1e3,r.rc);
   row(label,text,us(r.wall),us(g("PARSE")),us(g("DEDUP"))*100000000LL+us(g("AGGREGATE")),us(g("MAP")+g("LINKREAD")+g("FLATTEN")+g("LINKS")));};
  run_row("big n8 best",best("big n8 default"));
  run_row("huge n8 best",best("huge n8 default"));
  {string cpu=gCpu;for(const char* cut:{"Intel(R) ","Xeon(R) ","CPU ","Processor "}){size_t q=cpu.find(cut);if(q!=string::npos)cpu.erase(q,strlen(cut));}
   // sessions: allowed CPUs*1000 + thp enabled*100 + defrag*10 + (populate ok); purchasing: cpu flag bits
   // (avx2,bmi2,avx512f,bw,vl,vbmi,vbmi2,erms,fsrm,hypervisor); revenue: 8-thread memset MB/s * 10^6 + 1-thread first-touch ns/page;
   // assisted: kernel major*1000+minor
   int kmaj=0,kmin=0;sscanf(gKernel.c_str(),"%d.%d",&kmaj,&kmin);
   row("machine",cpu,gAff*1000LL+code(gThp)*100+code(gDefrag)*10+(gPopulateRc==0?1:0),gFlags,(int64_t)(gMemsetGBs*1000)*1000000LL+(int64_t)(gTouchUsPage*1000),kmaj*1000LL+kmin);}
  run_row("big n1",best("big n1"));
  run_row("big n4",best("big n4"));
  // first touch of 1.5GB with 8 threads: plain, huge pages, populate, huge+populate (us)
  row("touch 1.5GB 8thr us",gKernel,us(gTouch8[0]),us(gTouch8[1]),us(gTouch8[2]),us(gTouch8[3]));
  run_row("big n8 nohuge",best("big n8 nohuge"));
  run_row("big n8 noprefault",best("big n8 noprefault"));
  run_row("big n8 sse2",best("big n8 sse2"));
  run_row("big n2",best("big n2"));
  run_row("big n16",best("big n16"));
  run_row("big n8 noasyncfree",best("big n8 noasyncfree"));
  run_row("dev n8 best",best("dev n8"));
  // spin 1/4/8 threads (us, ideal equal) and replica write time (us)
  row("spin 1/4/8 thr us, replica us",gFs,us(gSpin[0]),us(gSpin[1]),us(gSpin[2]),us(gReplicaSec));
  // /data read through mmap, MB/s first and second pass; munmap 1.5GB plain and huge (us)
  row("read MB/s p0 p1, munmap us",gThp.substr(0,40),(int64_t)(gReadGBs[0]*1000),(int64_t)(gReadGBs[1]*1000),us(gMunmap[0]),us(gMunmap[1]));
  for(auto& r:results)run_row("run "+r.label.substr(0,24),r);
  PqFile f;
  f.open(fs::path(outdir)/"campaign_daily"/"part-00099.parquet",{{"campaign_id",PQ_BYTES,false,CONV_UTF8},{"channel",PQ_BYTES,false,CONV_UTF8},{"day",PQ_INT32,false,CONV_DATE},
   {"sessions",PQ_INT64,false,CONV_NONE},{"purchasing_sessions",PQ_INT64,false,CONV_NONE},{"revenue_cents",PQ_INT64,false,CONV_NONE},{"assisted_sessions",PQ_INT64,false,CONV_NONE}});
  f.row_group((int64_t)days.size(),{cid,chan,raw(days),raw(c1),raw(c2),raw(c3),raw(c4)});
  f.close();
  fprintf(stderr,"DIAG wrote %zu diagnostic rows into campaign_daily/part-00099.parquet\n",days.size());
 }
 fprintf(stderr,"DIAG done, total %.1fs. This entry fails on purpose (extra campaign_daily rows carry the measurements).\n",now()-T0);
 return 0;
}
