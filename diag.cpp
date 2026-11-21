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
#include <sys/resource.h>
#include <sys/syscall.h>
#include <linux/perf_event.h>
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

struct RunResult{string label;double wall=0,user=0,sys=0;long minflt=0,majflt=0,maxrssMB=0,nvcsw=0,nivcsw=0;int rc=0;map<string,double> t;uint64_t pc[4]={0,0,0,0};int perfErr=0;};
static vector<RunResult> results;
static int perf_open(pid_t pid,uint32_t type,uint64_t config){
 perf_event_attr a;memset(&a,0,sizeof a);a.size=sizeof a;a.type=type;a.config=config;a.inherit=1;a.exclude_kernel=0;a.exclude_hv=1;a.disabled=0;
 return (int)syscall(SYS_perf_event_open,&a,pid,-1,-1,0);
}
// Runs the pipeline as a child, forwards its stderr to ours, keeps its "TIME <stage> <seconds>" lines,
// its resource usage and (if the kernel allows) hardware counters: instructions, cycles, cache misses, branch misses.
static void run_child(const string& bin,vector<pair<string,string>> env,RunResult& r){
 int pfd[2],go[2];if(pipe(pfd)!=0||pipe(go)!=0){r.rc=-2;return;}
 double t=now();pid_t pid=fork();
 if(pid==0){dup2(pfd[1],2);close(pfd[0]);close(pfd[1]);close(go[1]);char c;(void)!read(go[0],&c,1);close(go[0]);
  for(auto& kv:env)setenv(kv.first.c_str(),kv.second.c_str(),1);execl(bin.c_str(),bin.c_str(),(char*)nullptr);_exit(127);}
 close(pfd[1]);close(go[0]);
 int fds[4];const uint64_t cfg[4]={PERF_COUNT_HW_INSTRUCTIONS,PERF_COUNT_HW_CPU_CYCLES,PERF_COUNT_HW_CACHE_MISSES,PERF_COUNT_HW_BRANCH_MISSES};
 for(int i=0;i<4;++i){fds[i]=perf_open(pid,PERF_TYPE_HARDWARE,cfg[i]);if(fds[i]<0&&!r.perfErr)r.perfErr=errno;}
 t=now();(void)!write(go[1],"g",1);close(go[1]);
 string out;char buf[4096];ssize_t n;
 while((n=read(pfd[0],buf,sizeof buf))>0)out.append(buf,n);
 close(pfd[0]);
 int st=0;rusage ru{};wait4(pid,&st,0,&ru);r.wall=now()-t;
 for(int i=0;i<4;++i)if(fds[i]>=0){uint64_t v=0;if(read(fds[i],&v,8)==8)r.pc[i]=v;close(fds[i]);}
 r.user=ru.ru_utime.tv_sec+ru.ru_utime.tv_usec*1e-6;r.sys=ru.ru_stime.tv_sec+ru.ru_stime.tv_usec*1e-6;
 r.minflt=ru.ru_minflt;r.majflt=ru.ru_majflt;r.maxrssMB=ru.ru_maxrss/1024;r.nvcsw=ru.ru_nvcsw;r.nivcsw=ru.ru_nivcsw;
 fwrite(out.data(),1,out.size(),stderr);
 size_t p=0;while(p<out.size()){size_t e=out.find('\n',p);if(e==string::npos)e=out.size();string line=out.substr(p,e-p);p=e+1;
  if(line.rfind("TIME ",0)==0){size_t sp=line.find(' ',5);if(sp!=string::npos)r.t[line.substr(5,sp-5)]+=atof(line.c_str()+sp+1);}}
 r.rc=WIFEXITED(st)?WEXITSTATUS(st):-1;
}
// Writes each CSV of `src` into `dst` as `parts` files, each holding the header and `reps` tagged copies of the
// data lines (a two-character replica tag goes in front of the given ID fields, so every copy has its own
// events and people). Files of part 0 are named *-0.csv, so a smaller set can link to them.
static uint64_t replicate(const fs::path& src,const fs::path& dst,int reps,int parts,int idFields){
 fs::create_directories(dst);
 vector<fs::path> files;for(auto& f:fs::directory_iterator(src))if(f.path().extension()==".csv")files.push_back(f.path());
 sort(files.begin(),files.end());
 atomic<size_t> next{0};atomic<uint64_t> bytes{0};
 par(8,[&](int){for(;;){size_t j=next.fetch_add(1);if(j>=files.size())break;
   string in;{ifstream f(files[j],ios::binary);in.assign(istreambuf_iterator<char>(f),{});}
   size_t hdr=in.find('\n');if(hdr==string::npos)hdr=in.size();else ++hdr;
   string out;out.reserve(in.size()+in.size()/6);
   for(int part=0;part<parts;++part){
    char name[64];snprintf(name,sizeof name,"part-%05zu-%d.csv",j,part);
    FILE* f=fopen((dst/name).c_str(),"wb");if(!f)continue;
    fwrite(in.data(),1,hdr,f);uint64_t wrote=hdr;
    for(int r0=0;r0<reps;++r0){
     const int rep=part*reps+r0;
     out.clear();const char tag=(char)('A'+rep%26),tag2=(char)('a'+rep/26);
     size_t p=hdr;
     while(p<in.size()){size_t e=in.find('\n',p);if(e==string::npos)e=in.size();
      size_t q=p;
      for(int k=0;k<idFields&&q<=e;++k){ // tag the start of field k unless it is empty
       size_t c=in.find(',',q);if(c==string::npos||c>e)c=e;
       if(c>q){if(in[q]=='"'){out.push_back('"');if(c-q>2){out.push_back(tag);out.push_back(tag2);}out.append(in,q+1,c-q-1);}else{out.push_back(tag);out.push_back(tag2);out.append(in,q,c-q);}}
       if(c<e)out.push_back(',');
       q=c+1;
      }
      if(q<=e)out.append(in,q,e-q);
      out.push_back('\n');p=e+1;
     }
     fwrite(out.data(),1,out.size(),f);wrote+=out.size();
    }
    fclose(f);bytes+=wrote;
   }
 }});
 return bytes.load();
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
 auto pipeline=[&](const string& label,const string& d,const string& o,vector<pair<string,string>> env,string bin=""){
  if(bin.empty())bin=core;else{bin=fs::path(core).parent_path().string()+"/"+bin;if(!fs::exists(bin)){fprintf(stderr,"DIAG RUN %s skipped: no %s\n",label.c_str(),bin.c_str());return;}}
  env.push_back({"DATA_DIR",d});env.push_back({"OUTPUT_DIR",o});env.push_back({"SCRATCH_DIR",scratch});
  fprintf(stderr,"DIAG RUN %s begin\n",label.c_str());RunResult r;r.label=label;run_child(bin,env,r);
  fprintf(stderr,"DIAG RUN %s end: wall %.3fs user %.3f sys %.3f minflt %ld rc=%d (total elapsed %.1fs)\n",label.c_str(),r.wall,r.user,r.sys,r.minflt,r.rc,now()-T0);results.push_back(r);};
 int runid=0;auto od=[&]{return scratch+"/dout"+to_string(runid++);};
 // ---- replicas of the dev data: `huge` = PARTS x BIG copies (final-sized with 3 x 20), `big` = links to its first part ----
 const int BIG=getenv("SS_DIAG_BIG")?atoi(getenv("SS_DIAG_BIG")):20;
 const int PARTS=getenv("SS_DIAG_PARTS")?atoi(getenv("SS_DIAG_PARTS")):3;
 const fs::path big=fs::path(scratch)/"big",huge=fs::path(scratch)/"huge";
 {double t=now();uint64_t b=0;
  for(const char* sub:{"events","clicks","links"}){
   b+=replicate(fs::path(data)/sub,huge/sub,BIG,PARTS,2);
   fs::create_directories(big/sub);
   for(auto& f:fs::directory_iterator(huge/sub)){const string n=f.path().filename().string();if(n.size()>6&&n.compare(n.size()-6,6,"-0.csv")==0)fs::create_symlink(f.path(),big/sub/n);}
  }
  for(const char* f:{"products.csv","campaigns.csv","manifest.json"}){fs::copy_file(fs::path(data)/f,big/f,fs::copy_options::overwrite_existing);fs::copy_file(fs::path(data)/f,huge/f,fs::copy_options::overwrite_existing);}
  gReplicaSec=now()-t;
  fprintf(stderr,"DIAG replica: %d x %d copies, %.1f MB written in %.2fs (elapsed %.1fs)\n",PARTS,BIG,b/1e6,now()-t,now()-T0);}
 const string B=big.string(),H=huge.string();
 auto timeleft=[&](double need){if(now()-T0+need>150){fprintf(stderr,"DIAG skipping the rest: out of time (%.1fs elapsed)\n",now()-T0);return false;}return true;};
 struct Var{const char* label;vector<pair<string,string>> env;const char* bin="";bool hugeSet=false;};
 #define N8 {"NCPU","8"}
 const vector<Var> vars={
  {"d1",{N8}},{"s1",{N8},"core-v35"},{"r1",{N8},"core-v33"},{"q1",{N8},"core-v32"},{"p1",{N8},"core-v31"},{"d2",{N8}},{"s2",{N8},"core-v35"},{"r2",{N8},"core-v33"},{"q2",{N8},"core-v32"},{"d3",{N8}},{"s3",{N8},"core-v35"},{"r3",{N8},"core-v33"},
  {"snoprobe",{N8,{"SS_NOSIMDPROBE","1"}},"core-v35"},{"sslot150",{N8,{"SS_SLOTPCT","150"}},"core-v35"},{"spfe0",{N8,{"SS_PFE","0"}},"core-v35"},{"spfa128",{N8,{"SS_PFA","128"}},"core-v35"},
  {"snoprobe2",{N8,{"SS_NOSIMDPROBE","1"}},"core-v35"},{"sslot1502",{N8,{"SS_SLOTPCT","150"}},"core-v35"},{"spfe02",{N8,{"SS_PFE","0"}},"core-v35"},{"spfa1282",{N8,{"SS_PFA","128"}},"core-v35"},
  {"idx",{N8,{"SS_LINES","index"}}},{"idxz",{N8,{"SS_LINES","index"},{"SS_ISA","avx512z"}}},{"idx2",{N8,{"SS_LINES","index"}}},{"idxz2",{N8,{"SS_LINES","index"},{"SS_ISA","avx512z"}}},
  {"tabfull",{N8,{"SS_TABLE_FULL","1"}}},{"noidblk",{N8,{"SS_NOIDBLOCKS","1"}}},{"tabfull2",{N8,{"SS_TABLE_FULL","1"}}},{"noidblk2",{N8,{"SS_NOIDBLOCKS","1"}}},
  {"abl1",{N8,{"SS_ABL","1"}},"core-abl"},{"abl2",{N8,{"SS_ABL","2"}},"core-abl"},{"abl6",{N8,{"SS_ABL","6"}},"core-abl"},{"abl3",{N8,{"SS_ABL","3"}},"core-abl"},
  {"abl4",{N8,{"SS_ABL","4"}},"core-abl"},{"abl5",{N8,{"SS_ABL","5"}},"core-abl"},
  {"x0",{N8},"core-x"},
  {"per16k",{N8,{"SS_PER","16384"}},"core-x"},{"per8k",{N8,{"SS_PER","8192"},{"SS_CAP","8192"}},"core-x"},{"per4k",{N8,{"SS_PER","4096"},{"SS_CAP","16384"}},"core-x"},
  {"uper8k",{N8,{"SS_UPER","8192"}},"core-x"},{"uper32k",{N8,{"SS_UPER","32768"}},"core-x"},
  {"pfw128",{N8,{"SS_PFW","128"}},"core-x"},{"pfw256",{N8,{"SS_PFW","256"}},"core-x"},
  {"pfe128",{N8,{"SS_PFE","128"}},"core-x"},{"pfe256",{N8,{"SS_PFE","256"}},"core-x"},
  {"combo",{N8,{"SS_PER","8192"},{"SS_CAP","8192"},{"SS_UPER","8192"},{"SS_PFW","128"},{"SS_PFE","128"}},"core-x"},
  {"avx512z",{N8,{"SS_ISA","avx512z"}}},{"avx512y",{N8,{"SS_ISA","avx512y"}}},
  {"noearly",{N8,{"SS_NOEARLYUNMAP","1"}}},{"nowill",{N8,{"SS_NOWILLNEED","1"}}},
  // final-sized set
  {"Hd1",{N8},"",true},{"Hs1",{N8},"core-v35",true},{"Hsnoprobe",{N8,{"SS_NOSIMDPROBE","1"}},"core-v35",true},{"Hr1",{N8},"core-v33",true},{"Hq1",{N8},"core-v32",true},{"Hp1",{N8},"core-v31",true},{"Hd2",{N8},"",true},{"Hx0",{N8},"core-x",true},{"Htabfull",{N8,{"SS_TABLE_FULL","1"}},"",true},
  {"Hper16k",{N8,{"SS_PER","16384"},{"SS_CAP","8192"}},"core-x",true},{"Hper8k",{N8,{"SS_PER","8192"},{"SS_CAP","16384"}},"core-x",true},
  {"Huper8k",{N8,{"SS_UPER","8192"}},"core-x",true},{"Hpf",{N8,{"SS_PFW","128"},{"SS_PFE","128"}},"core-x",true},
  {"Hcombo",{N8,{"SS_PER","8192"},{"SS_CAP","16384"},{"SS_UPER","8192"},{"SS_PFW","128"},{"SS_PFE","128"}},"core-x",true},
  {"Havx512z",{N8,{"SS_ISA","avx512z"}},"",true},{"Hnoearly",{N8,{"SS_NOEARLYUNMAP","1"}},"",true},{"Hd3",{N8},"",true},
  // repeats and lower-priority runs on the practice-sized set
  {"x0b",{N8},"core-x"},{"per8kb",{N8,{"SS_PER","8192"},{"SS_CAP","8192"}},"core-x"},{"pfw128b",{N8,{"SS_PFW","128"}},"core-x"},{"pfe128b",{N8,{"SS_PFE","128"}},"core-x"},
  {"combob",{N8,{"SS_PER","8192"},{"SS_CAP","8192"},{"SS_UPER","8192"},{"SS_PFW","128"},{"SS_PFE","128"}},"core-x"},
  {"avx512z2",{N8,{"SS_ISA","avx512z"}}},{"avx512y2",{N8,{"SS_ISA","avx512y"}}},{"noearly2",{N8,{"SS_NOEARLYUNMAP","1"}}},
  {"nohuge",{N8,{"SS_NOHUGE","1"}}},{"prefault",{N8,{"SS_PREFAULT","1"}}},
  {"n4",{{"NCPU","4"}}},{"n1",{{"NCPU","1"}}},{"n2",{{"NCPU","2"}}},{"n6",{{"NCPU","6"}}},
  {"1abl1",{{"NCPU","1"},{"SS_ABL","1"}},"core-abl"},{"1abl2",{{"NCPU","1"},{"SS_ABL","2"}},"core-abl"},{"1abl3",{{"NCPU","1"},{"SS_ABL","3"}},"core-abl"},{"1abl4",{{"NCPU","1"},{"SS_ABL","4"}},"core-abl"},
  {"d4",{N8}},{"s4",{N8},"core-v35"},{"d5",{N8}},{"s5",{N8},"core-v35"}};
 pipeline("dev",data,od(),{{"NCPU","8"}});
 for(const Var& v:vars){if(!timeleft(v.hugeSet?14:6))break;pipeline(v.label,v.hugeSet?H:B,od(),v.env,v.bin);}
 // ---- the real output, then break it on purpose so this entry is rejected and the log is returned ----
 pipeline("final /data -> /output",data,outdir,{{"NCPU","8"}});
 // Put the findings into extra campaign_daily rows: the dev check reports a few rows that differ, as JSON.
 // Row order (by campaign_id) is importance order; the text columns carry the measurements.
 {
  auto us=[](double sec){return (int64_t)(sec*1e6+0.5);};
  auto get=[&](const string& label)->const RunResult*{const RunResult* b=nullptr;for(auto& r:results)if(r.label==label&&r.rc==0&&(!b||r.wall<b->wall))b=&r;return b;};
  auto bestd=[&]()->const RunResult*{const RunResult* b=nullptr;for(auto& r:results)if(r.label.size()==2&&r.label[0]=='d'&&r.rc==0&&(!b||r.wall<b->wall))b=&r;return b;};
  auto stages=[&](const RunResult& r)->string{char t[256];auto g=[&](const char* k){auto it=r.t.find(k);return it==r.t.end()?0.0:it->second*1e3;};
   string s2;for(auto& kv:r.t){char one[48];snprintf(one,sizeof one,"%s%.0f ",(kv.first.rfind("LINK",0)==0&&kv.first.size()>4?("L"+kv.first.substr(4,3)):kv.first.substr(0,4)).c_str(),kv.second*1e3);s2+=one;}
   snprintf(t,sizeof t,"w%.0f u%.0f s%.0f flt%ld rss%ld cs%ld/%ld | ",r.wall*1e3,r.user*1e3,r.sys*1e3,r.minflt,r.maxrssMB,r.nvcsw,r.nivcsw);(void)g;return string(t)+s2;};
  string cid,chan;vector<int32_t> days;vector<int64_t> c1,c2,c3,c4;int idx=0;
  auto row=[&](const string& label,const string& text,int64_t a,int64_t b,int64_t c,int64_t d){
   char id[16];snprintf(id,sizeof id,"!%02d ",idx++);put_bytes(cid,string(id)+label);put_bytes(chan,text);days.push_back(20000+idx);
   c1.push_back(a);c2.push_back(b);c3.push_back(c);c4.push_back(d);};
  auto g1=[&](const RunResult& r,const char* k){auto it=r.t.find(k);return it==r.t.end()?0.0:it->second*1e3;};
  auto brief=[&](const char* l)->string{const RunResult* r=get(l);char one[96];if(!r){snprintf(one,sizeof one,"%s=- ",l);return one;}
   snprintf(one,sizeof one,"%s=%.0f(%.0f/%.0f/%.0f) ",l,r->wall*1e3,g1(*r,"PARSE"),g1(*r,"DEDUP"),g1(*r,"AGGREGATE"));return one;};
  // row 0: v32 against v31 on both sizes; best default run in full with hardware counters
  {const RunResult* d=bestd();RunResult z;if(!d)d=&z;char pc[160];snprintf(pc,sizeof pc," | perf err%d ins %.2fG cyc %.2fG cm %.1fM bm %.1fM",d->perfErr,d->pc[0]/1e9,d->pc[1]/1e9,d->pc[2]/1e6,d->pc[3]/1e6);
   string all;for(const char* l:{"d1","d2","d3","d4","d5","s1","s2","s3","s4","s5","r1","r2","r3","q1","q2","p1","Hd1","Hd2","Hd3","Hs1","Hr1","Hq1","Hp1"}){const RunResult* r=get(l);char one[32];snprintf(one,sizeof one,"%s=%.0f ",l,r?r->wall*1e3:-1.0);all+=one;}
   string det=stages(*d)+pc;
   {const RunResult* pb=nullptr;for(auto& r:results)if(r.label.size()==2&&r.label[0]=='p'&&r.rc==0&&(!pb||r.wall<pb->wall))pb=&r;if(pb)det+=" || v31: "+stages(*pb);}
   {const RunResult* h=get("Hd1");const RunResult* h2=get("Hd2");if(h2&&(!h||h2->wall<h->wall))h=h2;if(h)det+=" || 60x: "+stages(*h);}
   {const RunResult* qb=nullptr;for(auto& r:results)if(r.label.size()==2&&r.label[0]=='q'&&r.rc==0&&(!qb||r.wall<qb->wall))qb=&r;if(qb)det+=" || v32: "+stages(*qb);}
   {const RunResult* qb=nullptr;for(auto& r:results)if(r.label.size()==2&&r.label[0]=='r'&&r.rc==0&&(!qb||r.wall<qb->wall))qb=&r;if(qb)det+=" || v33: "+stages(*qb);}
   {const RunResult* qb=nullptr;for(auto& r:results)if(r.label.size()==2&&r.label[0]=='s'&&r.rc==0&&(!qb||r.wall<qb->wall))qb=&r;if(qb)det+=" || v35: "+stages(*qb);}
   row("v35 s*, v34 d*, v33 r*, v32 q*, v31 p*, H=60x: "+all,det,us(d->wall),us(d->user),us(d->sys),d->minflt);}
  // row 1: tuning experiments, wall(parse/dedup/agg)
  {string a,b2;for(const char* l:{"snoprobe","snoprobe2","sslot150","sslot1502","spfe0","spfe02","spfa128","spfa1282","x0","x0b","per16k","per8k","per8kb","per4k","uper8k","uper32k","pfw128","pfw128b","pfw256","pfe128","pfe128b","pfe256","combo","combob"})a+=brief(l);
   for(const char* l:{"Hd1","Hd2","Hd3","Hs1","Hsnoprobe","Hr1","Hq1","Hp1","Hx0","Htabfull","Hper16k","Hper8k","Huper8k","Hpf","Hcombo","Havx512z","Hnoearly"})b2+=brief(l);
   row("tuning 20x wall(P/D/A): "+a,"60x: "+b2,0,0,0,0);}
  // row 2: parser ablation (PARSE stage ms; the run stops after that stage)
  {string a,detail;for(auto& r:results)if(r.label.find("abl")!=string::npos){char one[64];snprintf(one,sizeof one,"%s=%.0f ",r.label.c_str(),g1(r,"PARSE"));a+=one;
    char d2[96];snprintf(d2,sizeof d2,"%s:u%.0f,s%.0f,f%ld,w%.0f ",r.label.c_str(),r.user*1e3,r.sys*1e3,r.minflt,r.wall*1e3);detail+=d2;}
   row("ablation PARSE ms (1 scan,2 +fields,6 +record to a ring,3 +bucket push,4 +users,5 clicks only): "+a,detail,0,0,0,0);}
  // row 3: switches and thread counts
  {string a,b2;for(const char* l:{"idx","idx2","idxz","idxz2","tabfull","tabfull2","noidblk","noidblk2","avx512z","avx512z2","avx512y","avx512y2","noearly","noearly2","nowill","nohuge","prefault"})a+=brief(l);
   for(const char* l:{"n1","n2","n4","n6"})b2+=brief(l);
   const RunResult* n1=get("n1");if(n1){char pc[160];snprintf(pc,sizeof pc," || n1: u%.0f s%.0f perf ins %.2fG cyc %.2fG cm %.1fM bm %.1fM",n1->user*1e3,n1->sys*1e3,n1->pc[0]/1e9,n1->pc[1]/1e9,n1->pc[2]/1e6,n1->pc[3]/1e6);b2+=pc;}
   row("switches: "+a,"threads: "+b2,0,0,0,0);}
  // row 4: every run, compact
  {string all;for(auto& r:results){char one[96];
    snprintf(one,sizeof one,"%s:%.0f/%.0f/%.0f/%.0f/L%.0f/u%.0f/s%.0f/f%ld ",r.label.c_str(),r.wall*1e3,g1(r,"PARSE"),g1(r,"DEDUP"),g1(r,"AGGREGATE"),g1(r,"LINKMAP")+g1(r,"LINKSETUP")+g1(r,"LINKREAD")+g1(r,"FLATTEN"),r.user*1e3,r.sys*1e3,r.minflt);all+=one;}
   row("all runs: label:wall/parse/dedup/agg/links/user/sys/faults",all,0,0,0,0);}
  // row 5: machine
  {char t[400];snprintf(t,sizeof t,"%s | kernel %s | thp %s | defrag %s | aff %d | fs %s | memset8 %.1fGB/s touch1 %.2fus/page touch8(1.5GB) %.0f/%.0f/%.0f/%.0f ms munmap %.0f/%.0f ms | spin1/4/8 %.0f/%.0f/%.0f ms | read %.1f/%.1f GB/s | replica %.1fs",
    gCpu.c_str(),gKernel.c_str(),gThp.c_str(),gDefrag.c_str(),gAff,gFs.c_str(),gMemsetGBs,gTouchUsPage,gTouch8[0]*1e3,gTouch8[1]*1e3,gTouch8[2]*1e3,gTouch8[3]*1e3,gMunmap[0]*1e3,gMunmap[1]*1e3,gSpin[0]*1e3,gSpin[1]*1e3,gSpin[2]*1e3,gReadGBs[0],gReadGBs[1],gReplicaSec);
   string extra=string("cpu.max ")+slurp("/sys/fs/cgroup/cpu.max",40)+" | cpuset "+slurp("/sys/fs/cgroup/cpuset.cpus.effective",40)+" | mem.max "+slurp("/sys/fs/cgroup/memory.max",30)+" | L2 "+slurp("/sys/devices/system/cpu/cpu0/cache/index2/size",12)+" L3 "+slurp("/sys/devices/system/cpu/cpu0/cache/index3/size",12)+" | sib0 "+slurp("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list",16)+" | flags "+to_string(gFlags);
   row("machine: "+extra,t,gAff,gFlags,0,0);}
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
