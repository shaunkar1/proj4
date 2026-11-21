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
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
using namespace std;
namespace fs=std::filesystem;
static double now(){return chrono::duration<double>(chrono::steady_clock::now().time_since_epoch()).count();}
static string slurp(const string& p,size_t maxb=4096){ifstream in(p,ios::binary);if(!in)return "<none>";string s(maxb,'\0');in.read(s.data(),maxb);s.resize(in.gcount());while(!s.empty()&&(s.back()=='\n'||s.back()==' '))s.pop_back();return s;}
static void show(const char* label,const string& path){fprintf(stderr,"DIAG %s: %s\n",label,slurp(path,300).c_str());}
template<class F> static double par(int T,F&& f){double t=now();vector<thread> th;for(int i=0;i<T;++i)th.emplace_back([&,i]{f(i);});for(auto& x:th)x.join();return now()-t;}
static int run_child(const string& bin,vector<pair<string,string>> env,double& wall){
 double t=now();pid_t pid=fork();
 if(pid==0){for(auto& kv:env)setenv(kv.first.c_str(),kv.second.c_str(),1);execl(bin.c_str(),bin.c_str(),(char*)nullptr);_exit(127);}
 int st=0;waitpid(pid,&st,0);wall=now()-t;return WIFEXITED(st)?WEXITSTATUS(st):-1;
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
int main(){
 setvbuf(stderr,nullptr,_IOLBF,0);
 const double T0=now();
 const string data=getenv("DATA_DIR")?getenv("DATA_DIR"):"/data",outdir=getenv("OUTPUT_DIR")?getenv("OUTPUT_DIR"):"/output",scratch=getenv("SCRATCH_DIR")?getenv("SCRATCH_DIR"):"/scratch";
 const string core=getenv("SS_CORE")?getenv("SS_CORE"):"/app/shopping-core";
 // ---- machine facts ----
 utsname u{};uname(&u);fprintf(stderr,"DIAG kernel: %s %s %s\n",u.sysname,u.release,u.machine);
 cpu_set_t cs;CPU_ZERO(&cs);sched_getaffinity(0,sizeof cs,&cs);
 string cpus;for(int i=0;i<CPU_SETSIZE;++i)if(CPU_ISSET(i,&cs))cpus+=to_string(i)+" ";
 fprintf(stderr,"DIAG nproc_conf=%ld nproc_onln=%ld affinity_count=%d cpus=[%s] NCPU=%s MEM_GB=%s\n",sysconf(_SC_NPROCESSORS_CONF),sysconf(_SC_NPROCESSORS_ONLN),CPU_COUNT(&cs),cpus.c_str(),getenv("NCPU")?getenv("NCPU"):"-",getenv("MEM_GB")?getenv("MEM_GB"):"-");
 {ifstream ci("/proc/cpuinfo");string line;int shown=0;string flags;
  while(getline(ci,line)){if(shown<1&&(line.rfind("model name",0)==0||line.rfind("cpu MHz",0)==0||line.rfind("cache size",0)==0||line.rfind("siblings",0)==0||line.rfind("cpu cores",0)==0||line.rfind("microcode",0)==0)){fprintf(stderr,"DIAG cpuinfo %s\n",line.c_str());}
   if(line.rfind("flags",0)==0&&flags.empty())flags=line;if(line.empty())++shown;}
  string keep;for(const char* f:{"sse4_2","avx2","bmi2","avx512f","avx512bw","avx512vl","avx512vbmi","avx512_vbmi2","gfni","vaes","vpclmulqdq","erms","fsrm","pdpe1gb","hypervisor","clwb","amx_tile","avx_vnni"})if(flags.find(string(" ")+f+" ")!=string::npos||flags.rfind(string(" ")+f)==flags.size()-strlen(f)-1)keep+=string(f)+" ";
  fprintf(stderr,"DIAG cpu flags: %s\n",keep.c_str());}
 for(int i=0;i<CPU_SETSIZE;++i)if(CPU_ISSET(i,&cs)){string b="/sys/devices/system/cpu/cpu"+to_string(i);
  fprintf(stderr,"DIAG cpu%d core_id=%s siblings=%s maxfreq=%s L2=%s L3=%s\n",i,slurp(b+"/topology/core_id",16).c_str(),slurp(b+"/topology/thread_siblings_list",32).c_str(),slurp(b+"/cpufreq/cpuinfo_max_freq",16).c_str(),slurp(b+"/cache/index2/size",16).c_str(),slurp(b+"/cache/index3/size",16).c_str());}
 show("thp enabled","/sys/kernel/mm/transparent_hugepage/enabled");show("thp defrag","/sys/kernel/mm/transparent_hugepage/defrag");show("thp khugepaged defrag","/sys/kernel/mm/transparent_hugepage/khugepaged/defrag");
 show("cgroup memory.max","/sys/fs/cgroup/memory.max");show("cgroup memory.high","/sys/fs/cgroup/memory.high");show("cgroup cpu.max","/sys/fs/cgroup/cpu.max");show("cgroup cpuset","/sys/fs/cgroup/cpuset.cpus.effective");show("cgroup v1 mem limit","/sys/fs/cgroup/memory/memory.limit_in_bytes");
 show("overcommit","/proc/sys/vm/overcommit_memory");show("fault_around","/sys/kernel/mm/transparent_hugepage/hpage_pmd_size");
 {ifstream mi("/proc/meminfo");string line;while(getline(mi,line))for(const char* k:{"MemTotal","MemFree","MemAvailable","Cached:","AnonHugePages","HugePages_Total","Hugepagesize"})if(line.rfind(k,0)==0)fprintf(stderr,"DIAG meminfo %s\n",line.c_str());}
 {ifstream mi("/proc/self/mountinfo");string line;while(getline(mi,line)){size_t d=line.find(" - ");string mp;{size_t a=0;for(int k=0;k<4;++k)a=line.find(' ',a)+1;mp=line.substr(a,line.find(' ',a)-a);}
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
  fprintf(stderr,"DIAG mem mode=%d (1.5GB, 8 thr): setup %.3fs populate_rc=%d first-touch %.3fs | memset %.3fs (%.1f GB/s) | memcpy 4thr %.3fs (%.1f GB/s) | munmap %.3fs | AnonHuge=%s\n",mode,setup,rc[0],touch,w2,8*CH/1e9/w2,cp,4*CH/1e9/cp,un,ah.c_str());
 }
 {char* m=(char*)mmap(nullptr,CH,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);double t=now();for(size_t k=0;k<CH;k+=4096)m[k]=1;double a=now()-t;t=now();memset(m,3,CH);double b=now()-t;munmap(m,CH);
  fprintf(stderr,"DIAG mem 1 thread 192MB: first-touch %.3fs (%.2f us/page) memset %.3fs (%.1f GB/s)\n",a,a/(CH/4096)*1e6,b,CH/1e9/b);}
 // spin benchmark: does 8-thread integer work scale? (hyperthread check)
 {auto spin=[](int){volatile uint64_t x=1;for(uint64_t i=0;i<400000000ULL;++i)x=x*6364136223846793005ULL+1442695040888963407ULL;};
  double t1=par(1,spin),t4=par(4,spin),t8=par(8,spin);fprintf(stderr,"DIAG spin: 1thr %.3fs 4thr %.3fs 8thr %.3fs (ideal = equal)\n",t1,t4,t8);}
 // file reading through mmap: first pass (maybe cold), second pass (warm)
 {vector<fs::path> files;for(auto& f:fs::directory_iterator(fs::path(data)/"events"))files.push_back(f.path());
  for(int pass=0;pass<2;++pass){atomic<size_t> next{0};atomic<uint64_t> bytes{0},sum{0};
   double t=par(8,[&](int){for(;;){size_t j=next.fetch_add(1);if(j>=files.size())break;int fd=open(files[j].c_str(),O_RDONLY);struct stat st;fstat(fd,&st);if(st.st_size){
     const char* b=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0);uint64_t s=0;for(off_t k=0;k<st.st_size;k+=64)s+=(unsigned char)b[k];sum+=s;bytes+=st.st_size;munmap((void*)b,st.st_size);}close(fd);}});
   fprintf(stderr,"DIAG read /data events pass %d: %.3fs %.1f MB (%.2f GB/s)\n",pass,t,bytes/1e6,bytes/1e9/t);}}
 // ---- pipeline on the dev data, several settings ----
 auto pipeline=[&](const string& label,const string& d,const string& o,vector<pair<string,string>> env){
  env.push_back({"DATA_DIR",d});env.push_back({"OUTPUT_DIR",o});env.push_back({"SCRATCH_DIR",scratch});
  fprintf(stderr,"DIAG RUN %s begin\n",label.c_str());double w;int rc=run_child(core,env,w);
  fprintf(stderr,"DIAG RUN %s end: wall %.3fs rc=%d (total elapsed %.1fs)\n",label.c_str(),w,rc,now()-T0);};
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
 {error_code ec;for(auto& f:fs::directory_iterator(fs::path(outdir)/"campaign_daily",ec))fs::remove(f.path(),ec);}
 fprintf(stderr,"DIAG done, total %.1fs. This entry fails on purpose (campaign_daily removed).\n",now()-T0);
 return 0;
}
