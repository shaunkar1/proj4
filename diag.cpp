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
#include <unordered_map>
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

static uint64_t gKe=0,gKu=0,gKc=0;static bool gKeys=false; // the generator's ID keys, read from the pipeline's STATS line
struct RunResult{string dth,csw;string hand;string raw;string label;double wall=0,user=0,sys=0;long minflt=0,majflt=0,maxrssMB=0,nvcsw=0,nivcsw=0;int rc=0;map<string,double> t;uint64_t pc[4]={0,0,0,0};int perfErr=0;};
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
 double tFirst=0,tLast=0;
 while((n=read(pfd[0],buf,sizeof buf))>0){tLast=now();if(tFirst==0)tFirst=tLast;out.append(buf,n);}
 close(pfd[0]);
 int st=0;rusage ru{};wait4(pid,&st,0,&ru);r.wall=now()-t;
 if(tFirst>0){r.t["zEXIT"]=t+r.wall-tLast;r.t["zFIRST"]=tFirst-t;} // last output -> reaped; start -> first output (the MAP mark)
 for(int i=0;i<4;++i)if(fds[i]>=0){uint64_t v=0;if(read(fds[i],&v,8)==8)r.pc[i]=v;close(fds[i]);}
 r.user=ru.ru_utime.tv_sec+ru.ru_utime.tv_usec*1e-6;r.sys=ru.ru_stime.tv_sec+ru.ru_stime.tv_usec*1e-6;
 r.minflt=ru.ru_minflt;r.majflt=ru.ru_majflt;r.maxrssMB=ru.ru_maxrss/1024;r.nvcsw=ru.ru_nvcsw;r.nivcsw=ru.ru_nivcsw;
 fwrite(out.data(),1,out.size(),stderr);
 size_t p=0;while(p<out.size()){size_t e=out.find('\n',p);if(e==string::npos)e=out.size();string line=out.substr(p,e-p);p=e+1;
  if(line.rfind("TIME ",0)==0){size_t sp=line.find(' ',5);if(sp!=string::npos)r.t[line.substr(5,sp-5)]+=atof(line.c_str()+sp+1);}
  if(line.rfind("CPU ",0)==0){size_t sp=line.find(' ',4);if(sp!=string::npos)r.t["c"+line.substr(4,sp-4)]+=atof(line.c_str()+sp+1);}
  if(line.rfind("SYS ",0)==0){size_t sp=line.find(' ',4);if(sp!=string::npos)r.t["s"+line.substr(4,sp-4)]+=atof(line.c_str()+sp+1);}
  if(line.rfind("FLT ",0)==0){size_t sp=line.find(' ',4);if(sp!=string::npos)r.t["f"+line.substr(4,sp-4)]+=atof(line.c_str()+sp+1)/1e6;}
  if(line.rfind("SMAPS FilePmdMapped:",0)==0)r.t["pmd"]=atof(line.c_str()+20)/1e6;
  for(const char* pre:{"dense:","STATS dense keys","STATS ev8 loop","STATS cl8","STATS dense reader","linked IDs","STATS event buckets","STATS click buckets","ERROR"})
   if(line.rfind(pre,0)==0&&r.raw.size()<700){r.raw+=line.substr(0,150);r.raw+=" # ";}
  if(line.rfind("STATS dense threads:",0)==0)r.dth=line.substr(20);
  if(line.rfind("STATS dense reader:",0)==0){size_t a=line.find("handed over ");size_t b=line.find(", deferred");if(a!=string::npos&&b!=string::npos&&b>a)r.hand=line.substr(a+12,b-a-12);size_t c=line.find("slots ");if(c!=string::npos)r.hand+=" of "+line.substr(c+6,line.find(' ',c+6)-c-6);}
  if(line.rfind("CSW DENSE ",0)==0)r.csw=line.substr(10);
  if(line.rfind("STATS dense keys ",0)==0){unsigned long long a=0,b=0,c=0;if(sscanf(line.c_str(),"STATS dense keys %llu %llu %llu",&a,&b,&c)==3&&(a|b|c)){gKe=a;gKu=b;gKc=c;gKeys=true;}}}
 r.rc=WIFEXITED(st)?WEXITSTATUS(st):-1;
}
// ---- The generator's ID scheme (see the dn:: block in main.cpp): needed to make replicas whose IDs are
// still counters, so the pipeline's dense route sees them the way it sees real generated data. ----
static const uint64_t DM=(1ull<<40)-1,DM1=0x9E3779B97F4Bull&DM,DM2=0xD6E8FEB86659ull&DM;
static uint64_t dinv(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&DM;}
static const uint64_t DI1=dinv(DM1),DI2=dinv(DM2);
static inline uint64_t d_index(uint64_t y,uint64_t key){uint64_t m=(y*DI2)&DM;m=m^(m>>17)^(m>>34);return ((m^key)*DI1)&DM;}
static inline uint64_t d_value(uint64_t index,uint64_t key){uint64_t m=((index*DM1)^key)&DM;m^=m>>17;return (m*DM2)&DM;}
static inline bool d_parse36(const char* s,size_t n,uint64_t& y){if(n-1>7)return false;if(s[0]=='0'&&n>1)return false;uint64_t v=0;
 for(size_t i=0;i<n;++i){unsigned d=(unsigned char)s[i]-(unsigned)'0';if(d>9){d=(unsigned char)s[i]-(unsigned)'a';if(d>25)return false;d+=10;}v=v*36+d;}
 if(v>DM)return false;y=v;return true;}
static inline void d_text36(string& out,char letter,uint64_t y){char buf[8];int at=8;do{const unsigned d=(unsigned)(y%36);buf[--at]=(char)(d<10?'0'+d:'a'+(d-10));y/=36;}while(y);out.push_back(letter);out.append(buf+at,(size_t)(8-at));}
static atomic<uint64_t> gMaxE{0},gMaxC{0},gMaxI{0}; // largest counters / user index in the source data (plausible ones only)
static uint64_t gOffE=0,gOffC=0,gOffI=0;            // counter distance between two copies
// Counter of an ID field (quotes already removed) if it is a canonical ID with this letter; false otherwise.
static inline bool d_counter(const char* s,size_t n,char letter,uint64_t& v){
 if(!gKeys||n<2||s[0]!=letter)return false;uint64_t y;if(!d_parse36(s+1,n-1,y))return false;
 v=d_index(y,letter=='e'?gKe:letter=='k'?gKc:gKu);return true;}
// Bots: the generator gives a few users about 1% of all events each, at any scale. A plain replica turns each into
// many small users; with SS_DIAG_BOTS (default on) the copies of such a user stay ONE user whose events (and clicks)
// are consecutive counters again, as in real data of that size: counter x of the user's range [f,l) in the source
// becomes base + (x-f)*copies + copy, in a range of its own above all other counters.
struct BotInfo{uint64_t ef=~0ull,el=0,cf=~0ull,cl=0,ne=0,nc=0;};
static unordered_map<uint64_t,BotInfo> gBots; // by user index
static uint64_t gBotRows=0;
static void d_find_bots(const fs::path& data,double share){
 vector<BotInfo> info(gOffI);uint64_t rows=0;
 for(int kind=0;kind<2;++kind){
  for(auto& f:fs::directory_iterator(data/(kind?"clicks":"events"))){if(f.path().extension()!=".csv")continue;
   string in;{ifstream g(f.path(),ios::binary);in.assign(istreambuf_iterator<char>(g),{});}
   size_t p=in.find('\n');if(p==string::npos)continue;++p;
   while(p<in.size()){size_t e=in.find('\n',p);if(e==string::npos)e=in.size();
    size_t c0=in.find(',',p);if(c0!=string::npos&&c0<e){size_t c1=in.find(',',c0+1);if(c1==string::npos||c1>e)c1=e;
     size_t a=p,b=c0,ua=c0+1,ub=c1;if(b-a>=2&&in[a]=='"'&&in[b-1]=='"'){++a;--b;}if(ub-ua>=2&&in[ua]=='"'&&in[ub-1]=='"'){++ua;--ub;}
     uint64_t x,u;if(!kind)++rows;
     if(d_counter(in.data()+ua,ub-ua,'u',u)&&(u>>9)<gOffI&&d_counter(in.data()+a,b-a,kind?'k':'e',x)&&x<(kind?gOffC:gOffE)){BotInfo& bi=info[u>>9];
      if(kind){bi.cf=min(bi.cf,x);bi.cl=max(bi.cl,x+1);++bi.nc;}else{bi.ef=min(bi.ef,x);bi.el=max(bi.el,x+1);++bi.ne;}}}
    p=e+1;}}}
 for(uint64_t i=0;i<gOffI;++i)if(info[i].ne>=share*rows&&info[i].ne>=1000){gBots[i]=info[i];gBotRows+=info[i].ne;
  fprintf(stderr,"DIAG bot index %llu: %llu events in counters [%llu,%llu), %llu clicks in [%llu,%llu)\n",(unsigned long long)i,(unsigned long long)info[i].ne,(unsigned long long)info[i].ef,(unsigned long long)info[i].el,(unsigned long long)info[i].nc,(unsigned long long)(info[i].nc?info[i].cf:0),(unsigned long long)info[i].cl);}
 fprintf(stderr,"DIAG bots: %zu users with %llu of %llu event rows\n",gBots.size(),(unsigned long long)gBotRows,(unsigned long long)rows);
}
static void d_scan_max(const fs::path& dir,char l0,char l1){
 vector<fs::path> files;for(auto& f:fs::directory_iterator(dir))if(f.path().extension()==".csv")files.push_back(f.path());
 atomic<size_t> next{0};
 par(8,[&](int){uint64_t mE=0,mC=0,mI=0;for(;;){size_t j=next.fetch_add(1);if(j>=files.size())break;
   string in;{ifstream f(files[j],ios::binary);in.assign(istreambuf_iterator<char>(f),{});}
   size_t p=in.find('\n');if(p==string::npos)continue;++p;
   while(p<in.size()){size_t e=in.find('\n',p);if(e==string::npos)e=in.size();size_t q=p;
    for(int k=0;k<2&&q<=e;++k){size_t c=in.find(',',q);if(c==string::npos||c>e)c=e;size_t a=q,b=c;if(b-a>=2&&in[a]=='"'&&in[b-1]=='"'){++a;--b;}
     const char letter=k?l1:l0;uint64_t v;
     if(d_counter(in.data()+a,b-a,letter,v)){if(letter=='u'){if((v>>9)<(1ull<<26))mI=max(mI,v>>9);}else if(v<(1ull<<31)){if(letter=='e')mE=max(mE,v);else mC=max(mC,v);}}
     q=c+1;}
    p=e+1;}}
  for(auto pr:{make_pair(&gMaxE,mE),make_pair(&gMaxC,mC),make_pair(&gMaxI,mI)}){uint64_t cur=pr.first->load();while(pr.second>cur&&!pr.first->compare_exchange_weak(cur,pr.second)){}}});
}
// ---- v5 data: IDs are opaque (letter + up to 8 lowercase base-36 characters, or letter + 32 lowercase hex). A copy
// gets every canonical ID pushed through a per-copy bijection of its own form (so copies are distinct and IDs stay
// canonical); the user IDs of the heaviest users ("whales") stay the same in every copy, so a whale of the replica
// has the weight a whale has in real data of that size. Other texts get a two-character tag as before.
static bool gV5=false;
static unordered_map<string,int> gWhaleIds;
static const uint64_t M36=2821109907456ull; // 36^8
static inline bool v5_short(const char* s,size_t n,uint64_t& v){if(n<2||n>9)return false;v=0;for(size_t i=1;i<n;++i){unsigned d=(unsigned char)s[i]-(unsigned)'0';if(d>9){d=(unsigned char)s[i]-(unsigned)'a';if(d>25)return false;d+=10;}v=v*36+d;}return true;}
static inline bool v5_long(const char* s,size_t n,unsigned char* out16){if(n!=33)return false;for(int i=0;i<16;++i){unsigned hi=(unsigned char)s[1+2*i],lo=(unsigned char)s[2+2*i];
  auto hx=[](unsigned c)->int{if(c-'0'<10)return (int)(c-'0');if(c-'a'<6)return (int)(c-'a'+10);return -1;};const int h=hx(hi),l=hx(lo);if(h<0||l<0)return false;out16[i]=(unsigned char)(h*16+l);}return true;}
static inline void v5_put_short(string& out,char letter,uint64_t v,uint64_t rep){
 uint64_t A=(0x9E3779B97F4A7C15ull*(rep+1))%M36;A=A-(A%6)+1;const uint64_t B=(0xD6E8FEB86659FD93ull*(rep+7))%M36;
 const uint64_t w=(uint64_t)(((unsigned __int128)v*A+B)%M36);
 char buf[8];int at=8;uint64_t y=w;do{const unsigned d=(unsigned)(y%36);buf[--at]=(char)(d<10?'0'+d:'a'+(d-10));y/=36;}while(y);out.push_back(letter);out.append(buf+at,(size_t)(8-at));}
static inline void v5_put_long(string& out,char letter,const unsigned char* b16,uint64_t rep){
 static const char* H="0123456789abcdef";out.push_back(letter);uint64_t x=0x243F6A8885A308D3ull*(rep+1);
 for(int i=0;i<16;++i){x^=x>>29;x*=0xBF58476D1CE4E5B9ull;x^=x>>32;const unsigned char c=(unsigned char)(b16[i]^(unsigned char)(x>>(8*(i&7))));out.push_back(H[c>>4]);out.push_back(H[c&15]);}}
static void v5_find_whales(const fs::path& data,size_t top){
 unordered_map<string,uint32_t> cnt;
 for(auto& f:fs::directory_iterator(data/"events")){if(f.path().extension()!=".csv")continue;
  string in;{ifstream g(f.path(),ios::binary);in.assign(istreambuf_iterator<char>(g),{});}
  size_t p=in.find('\n');if(p==string::npos)continue;++p;
  while(p<in.size()){size_t e=in.find('\n',p);if(e==string::npos)e=in.size();size_t c0=in.find(',',p);
   if(c0!=string::npos&&c0<e){size_t c1=in.find(',',c0+1);if(c1==string::npos||c1>e)c1=e;size_t ua=c0+1,ub=c1;if(ub-ua>=2&&in[ua]=='"'&&in[ub-1]=='"'){++ua;--ub;}if(ub>ua)++cnt[in.substr(ua,ub-ua)];}
   p=e+1;}}
 vector<pair<uint32_t,string>> v;v.reserve(cnt.size());for(auto& kv:cnt)v.push_back({kv.second,kv.first});
 sort(v.begin(),v.end(),[](auto& x,auto& y){return x.first>y.first;});
 uint64_t rows=0;for(size_t i=0;i<v.size()&&i<top;++i){gWhaleIds[v[i].second]=1;rows+=v[i].first;}
 fprintf(stderr,"DIAG v5 whales: %zu user IDs kept in all copies, %llu event rows (heaviest %u, lightest kept %u), distinct user IDs %zu\n",gWhaleIds.size(),(unsigned long long)rows,v.empty()?0u:v[0].first,v.size()>top?v[top-1].first:0u,v.size());
 gBotRows=rows;
}
// Writes each CSV of `src` into `dst` as `parts` files, each holding the header and `reps` copies of the data
// lines. In copy r every dense ID in the first two fields is moved r steps up its counter space (events and
// clicks by the number of counters in the source, users by the number of user indexes), so the copies are
// distinct and the whole replica is again dense; any other ID gets a two-character replica tag in front.
// Files of part 0 are named *-0.csv, so a smaller set can link to them.
static uint64_t replicate(const fs::path& src,const fs::path& dst,int reps,int parts,char l0,char l1,bool bots=false){
 // bot ranges of this replica: copies = reps*parts, bases above every other counter
 const uint64_t copies=(uint64_t)reps*parts;
 struct BotMap{uint64_t ef,el,cf,cl,baseE,baseC;};unordered_map<uint64_t,BotMap> bm;
 if(bots){uint64_t be=copies*gOffE,bc=copies*gOffC;vector<uint64_t> keys;for(auto& kv:gBots)keys.push_back(kv.first);sort(keys.begin(),keys.end());
  for(uint64_t k:keys){const BotInfo& b=gBots[k];BotMap m{b.ef,b.el,b.cf,b.cl,be,bc};if(b.ne)be+=(b.el-b.ef)*copies;if(b.nc)bc+=(b.cl-b.cf)*copies;bm[k]=m;}}
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
    // The copies are interleaved line by line, each starting at another place of the source file: neighbouring
    // rows then hit counters all over the range, as rows of real data do (copy after copy would keep ~37k rows
    // in a row inside one copy's 1/60 of the slot array).
    vector<size_t> ls;{size_t p=hdr;while(p<in.size()){ls.push_back(p);size_t e=in.find('\n',p);if(e==string::npos)break;p=e+1;}}
    const size_t nl=ls.size();out.clear();
    for(size_t t=0;t<nl;++t)for(int r0=0;r0<reps;++r0){
     const uint64_t rep=(uint64_t)part*reps+r0;
     const char tag=(char)('A'+rep%26),tag2=(char)('a'+rep/26);
     size_t p=ls[(t+(size_t)r0*(nl/(size_t)reps))%nl];
     {size_t e=in.find('\n',p);if(e==string::npos)e=in.size();
      size_t q=p;
      // the line's user (second field; in link files both fields are users): is it a bot of this replica?
      const BotMap* bot=nullptr;
      if(!bm.empty()&&l0!='u'){size_t c0=in.find(',',p);if(c0!=string::npos&&c0<e){size_t c1=in.find(',',c0+1);if(c1==string::npos||c1>e)c1=e;size_t ua=c0+1,ub=c1;if(ub-ua>=2&&in[ua]=='"'&&in[ub-1]=='"'){++ua;--ub;}
        uint64_t u;if(d_counter(in.data()+ua,ub-ua,'u',u)){auto it=bm.find(u>>9);if(it!=bm.end())bot=&it->second;}}}
      for(int k=0;k<2&&q<=e;++k){
       size_t c=in.find(',',q);if(c==string::npos||c>e)c=e;
       if(c>q){size_t a=q,b=c;const bool quoted=b-a>=2&&in[a]=='"'&&in[b-1]=='"';if(quoted){++a;--b;}
        const char letter=k?l1:l0;uint64_t v;bool dense=false,keep=false,odd=false;
        bool v5done=false;
        if(gV5&&rep&&b>a&&in[a]==letter&&!(in[q]=='"'&&!quoted)){
         uint64_t sv;unsigned char b16[16];
         if(letter=='u'&&gWhaleIds.count(in.substr(a,b-a))){} // a whale's user ID: the same in every copy (written as it is below: keep)
         else if(v5_short(in.data()+a,b-a,sv)){if(quoted)out.push_back('"');v5_put_short(out,letter,sv,rep);if(quoted)out.push_back('"');v5done=true;}
         else if(v5_long(in.data()+a,b-a,b16)){if(quoted)out.push_back('"');v5_put_long(out,letter,b16,rep);if(quoted)out.push_back('"');v5done=true;}
        }
        if(gV5&&rep&&!v5done&&letter=='u'&&b>a&&gWhaleIds.count(in.substr(a,b-a)))keep=true;
        if(v5done){}
        else if(!gV5&&(rep||bot||!bm.empty())&&d_counter(in.data()+a,b-a,letter,v)){
         if(letter=='u'){if(!bm.empty()&&bm.count(v>>9))keep=true; // a bot's user ID is the same in every copy
          else if(rep&&(v>>9)<gOffI){v+=(rep*gOffI)<<9;dense=true;}}
         else if(bot&&letter=='e'&&v>=bot->ef&&v<bot->el){
          // about 10 rows per million of a bot get an odd event ID (real data: a few odd IDs per million rows put a bot's index into the general lane)
          if(((v*0x9E3779B97F4A7C15ull+rep*0xD6E8FEB86659FD93ull)>>20)%100000==0){odd=true;}
          else{v=bot->baseE+(v-bot->ef)*copies+rep;dense=true;}}
         else if(bot&&letter=='k'&&v>=bot->cf&&v<bot->cl){v=bot->baseC+(v-bot->cf)*copies+rep;dense=true;}
         else if(rep){const uint64_t off=letter=='e'?gOffE:gOffC;if(v<off){v+=rep*off;dense=true;}}}
        if(v5done){}
        else if(in[q]=='"'&&!quoted){out.append(in,q,c-q);} // a lone quote: leave the field alone
        else{if(quoted)out.push_back('"');
         if(odd){out.push_back('X');out.push_back((char)('A'+rep%26));out.push_back((char)('a'+rep/26));out.append(in,a,b-a);}
         else if(keep||(!rep&&!dense))out.append(in,a,b-a);
         else if(dense)d_text36(out,letter,d_value(v,letter=='e'?gKe:letter=='k'?gKc:gKu));
         else{if(b>a){out.push_back(tag);out.push_back(tag2);}out.append(in,a,b-a);}
         if(quoted)out.push_back('"');}
       }
       if(c<e)out.push_back(',');
       q=c+1;
      }
      if(q<=e)out.append(in,q,e-q);
      out.push_back('\n');
     }
     if(out.size()>(4u<<20)){fwrite(out.data(),1,out.size(),f);wrote+=out.size();out.clear();}
    }
    fwrite(out.data(),1,out.size(),f);wrote+=out.size();out.clear();
    fclose(f);bytes+=wrote;
   }
 }});
 return bytes.load();
}
// ---- Micro-benchmark for the dense route: what is the cheapest way to get rows into an array indexed by
// event counter? (a) scatter 32-byte records into buckets of 16384 counters, then a second pass that
// writes each bucket's winners by direct index; (b) write each row straight into a global slot array with a
// software prefetch issued d rows earlier. Rows arrive in random counter order, 3% are second copies, and a
// dependent multiply chain of about 120 cycles stands in for parsing the line. Times in ms, T threads.
struct BRec{uint64_t k;int64_t ing;uint32_t ve;uint32_t user;uint32_t cat;uint32_t pad;};
struct BSlot{atomic<uint64_t> k;uint64_t uc;int64_t ing;};
static void* hmap(size_t bytes){const size_t H=size_t(2)<<20,want=(bytes+H-1)&~(H-1);void* m=mmap(nullptr,want+H,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
 if(m==MAP_FAILED)return nullptr;char* a=(char*)(((uintptr_t)m+H-1)&~(uintptr_t)(H-1));madvise(a,want,MADV_HUGEPAGE);return a;}
static inline uint64_t bwork(uint64_t x){for(int i=0;i<24;++i){x*=0x9E3779B97F4A7C15ull;x^=x>>29;}return x;}
static inline BRec brow(uint64_t i,uint64_t N){ // row i of the stream
 uint64_t ve=(i*1000003ull)%N;if((i&31)==7)ve=((i-5)*1000003ull)%N; // a copy of a row five lines earlier
 const uint64_t w=bwork(i+ve);BRec r;r.k=w|1;r.ing=(int64_t)(w>>20);r.ve=(uint32_t)ve;r.user=(uint32_t)w;r.cat=(uint32_t)(w>>40)&0xffff;r.pad=0;return r;}
static vector<double> bench_dense(uint64_t N,int T){
 vector<double> out;atomic<uint64_t> sink{0};
 out.push_back(1e3*par(T,[&](int t){uint64_t s=0;for(uint64_t i=N*t/T,e=N*(t+1)/T;i<e;++i)s+=brow(i,N).k;sink+=s;}));
 { // (a) scatter, then direct-index pass per bucket
  const uint64_t NB=(N>>14)+1,per=(N/T/NB)*5/4+64;
  vector<BRec*> base(T),tails(T*NB);
  for(int t=0;t<T;++t){base[t]=(BRec*)hmap(NB*per*sizeof(BRec));if(!base[t])return out;for(uint64_t b=0;b<NB;++b)tails[t*NB+b]=base[t]+b*per;}
  out.push_back(1e3*par(T,[&](int t){BRec** tl=&tails[t*NB];BRec* const b0=base[t];
   for(uint64_t i=N*t/T,e=N*(t+1)/T;i<e;++i){const BRec r=brow(i,N);const uint64_t b=r.ve>>14;BRec* p=tl[b];if(p<b0+(b+1)*per){*p=r;__builtin_prefetch((const char*)p+64,1,3);tl[b]=p+1;}}}));
  uint64_t* D=(uint64_t*)hmap(N*16);if(!D)return out;atomic<uint64_t> next{0};
  out.push_back(1e3*par(T,[&](int){vector<int64_t> ing(16384);
   for(;;){const uint64_t b=next.fetch_add(1);if(b>=NB)break;fill(ing.begin(),ing.end(),INT64_MAX);
    for(int t=0;t<T;++t)for(BRec* p=base[t]+b*per,*e=tails[t*NB+b];p<e;++p){const uint32_t i=p->ve&16383;if(p->ing<ing[i]){ing[i]=p->ing;D[2*(uint64_t)p->ve]=p->k;D[2*(uint64_t)p->ve+1]=((uint64_t)p->cat<<32)|p->user;}}}}));
  sink+=D[2*(N/2)];munmap(D,N*16);for(int t=0;t<T;++t)munmap(base[t],NB*per*sizeof(BRec));
 }
 for(int d:{1,2,4,8}){ // (b) straight into the global slot array, prefetch d rows ahead
  BSlot* S=(BSlot*)hmap(N*sizeof(BSlot));if(!S)return out;
  out.push_back(1e3*par(T,[&](int t){BRec ring[8];const uint64_t i0=N*t/T,e=N*(t+1)/T;
   auto commit=[&](const BRec& r){BSlot& sl=S[r.ve];uint64_t cur=sl.k.load(memory_order_relaxed);
    for(;;){if(cur==0){if(sl.k.compare_exchange_weak(cur,1)){sl.uc=((uint64_t)r.cat<<32)|r.user;sl.ing=r.ing;sl.k.store(r.k,memory_order_release);return;}}
     else if(cur==1){cur=sl.k.load(memory_order_relaxed);} // another thread is writing this slot
     else{if(sl.k.compare_exchange_weak(cur,1)){if(r.ing<sl.ing){sl.uc=((uint64_t)r.cat<<32)|r.user;sl.ing=r.ing;sl.k.store(r.k,memory_order_release);}else sl.k.store(cur,memory_order_release);return;}}}};
   for(uint64_t i=i0;i<e;++i){const BRec r=brow(i,N);__builtin_prefetch(&S[r.ve],1,3);if(i-i0>=(uint64_t)d)commit(ring[(i-d)&7]);ring[i&7]=r;}
   for(uint64_t i=(e-i0>=(uint64_t)d?e-d:i0);i<e;++i)commit(ring[i&7]);}));
  sink+=S[N/2].uc;munmap(S,N*sizeof(BSlot));
 }
 fprintf(stderr,"DIAG bench_dense N=%llu T=%d:",(unsigned long long)N,T);for(double v:out)fprintf(stderr," %.0f",v);fprintf(stderr," (sink %llu)\n",(unsigned long long)sink.load());
 return out;
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
 const bool quick=getenv("SS_DIAG_QUICK")&&atoi(getenv("SS_DIAG_QUICK"))!=0; // this diagnostic: what does the new public set look like to the pipeline?
 pipeline("keys",data,od(),{{"NCPU","8"},{"SS_STATS","1"}});
 string sampleText;
 for(const char* sub:{"events","clicks","links"}){vector<fs::path> fl;error_code ec;for(auto& f:fs::directory_iterator(fs::path(data)/sub,ec))fl.push_back(f.path());sort(fl.begin(),fl.end());
  if(fl.empty())continue;ifstream g(fl[0],ios::binary);string buf(230,'\0');g.read(&buf[0],230);buf.resize((size_t)g.gcount());for(char& ch:buf)if(ch=='\n')ch='|';else if(ch=='"')ch='`';else if((unsigned char)ch<32||(unsigned char)ch>126)ch='?';
  sampleText+=string(sub)+"("+to_string(fl.size())+" files): "+buf+" ## ";}
 fprintf(stderr,"DIAG samples: %s\n",sampleText.c_str()); // the pipeline finds the generator's keys; its STATS line gives them to us
 if(gKeys){d_scan_max(fs::path(data)/"events",'e','u');d_scan_max(fs::path(data)/"clicks",'k','u');d_scan_max(fs::path(data)/"links",'u','u');
  gOffE=gMaxE.load()+1;gOffC=gMaxC.load()+1;gOffI=gMaxI.load()+1;}
 fprintf(stderr,"DIAG keys %d: counters per copy: events %llu clicks %llu user indexes %llu\n",(int)gKeys,(unsigned long long)gOffE,(unsigned long long)gOffC,(unsigned long long)gOffI);
 {double t=now();uint64_t b=0;
  if(!quick){
  gV5=!gKeys;if(gV5)v5_find_whales(fs::path(data),40);
  const bool botMode=gKeys&&!(getenv("SS_DIAG_BOTS")&&atoi(getenv("SS_DIAG_BOTS"))==0);
  if(botMode)d_find_bots(fs::path(data),0.005);
  for(const char* sub:{"events","clicks","links"}){
   const char l0=sub[0]=='e'?'e':sub[0]=='c'?'k':'u';
   b+=replicate(fs::path(data)/sub,huge/sub,BIG,PARTS,l0,'u',botMode);
   fs::create_directories(big/sub);
   if(gV5)b+=replicate(fs::path(data)/sub,big/sub,BIG,1,l0,'u',false); // v5: its own files (line-interleaved copies 0..BIG-1)
   else if(botMode&&!gBots.empty())b+=replicate(fs::path(data)/sub,big/sub,BIG,1,l0,'u',true); // its own files: the bots' ranges depend on the number of copies
   else for(auto& f:fs::directory_iterator(huge/sub)){const string n=f.path().filename().string();if(n.size()>6&&n.compare(n.size()-6,6,"-0.csv")==0)fs::create_symlink(f.path(),big/sub/n);}
  }
  for(const char* f:{"products.csv","campaigns.csv","manifest.json"}){fs::copy_file(fs::path(data)/f,big/f,fs::copy_options::overwrite_existing);fs::copy_file(fs::path(data)/f,huge/f,fs::copy_options::overwrite_existing);}
  }
  gReplicaSec=now()-t;
  fprintf(stderr,"DIAG replica: %d x %d copies, %.1f MB written in %.2fs (elapsed %.1fs)\n",PARTS,BIG,b/1e6,now()-t,now()-T0);}
 // The public set as the scorer mounts it (/data) against a copy of it on the scratch mount: do file mappings behave the same?
 const fs::path devcopy=fs::path(scratch)/"devcopy";
 {error_code ec;fs::create_directories(devcopy,ec);fs::copy(fs::path(data),devcopy,fs::copy_options::recursive|fs::copy_options::overwrite_existing,ec);if(ec)fprintf(stderr,"DIAG devcopy failed: %s\n",ec.message().c_str());}
 double syncSec=0;{const double t=now();int fd=open(scratch.c_str(),O_RDONLY|O_DIRECTORY);if(fd>=0){syncfs(fd);close(fd);}syncSec=now()-t;fprintf(stderr,"DIAG syncfs of the scratch mount after the replica: %.2fs\n",syncSec);}
 const string B=big.string(),H=huge.string();
 auto timeleft=[&](double need){if(now()-T0+need>150){fprintf(stderr,"DIAG skipping the rest: out of time (%.1fs elapsed)\n",now()-T0);return false;}return true;};
 struct Var{string label;vector<pair<string,string>> env;const char* bin="";bool hugeSet=false;};
 #define N8 {"NCPU","8"}
 vector<Var> vars;
 auto add=[&](const char* group,int reps,vector<pair<string,string>> env,const char* bin="",bool hs=false){for(int i=0;i<reps;++i)vars.push_back({string(group)+"#"+to_string(i),env,bin,hs});};
 // practice-sized set: the first run only warms the page cache
 add("warm",1,{N8});
 #define V3 {"SS_EV8","3"}
 #define ST {"SS_STATS","1"}
 #define PO {"SS_DENSE_DUMP","-"}
 #define UB {"SS_UNMAP","bg"}
 #define L2 {"SS_EVLEAD","2"},{"SS_CLLEAD","2"}
 #define NL {"SS_LANES","0"}
 #define ND {"SS_NODENSE","1"}
 for(int r=0;r<3;++r){
  add("Bs",1,{N8,ST});add("B",1,{N8});
  if(r<2){add("n4",1,{{"NCPU","4"}});add("n16",1,{{"NCPU","16"}});add("p4",1,{N8,{"SS_PERSONBUCKET","4096"}});add("p64",1,{N8,{"SS_PERSONBUCKET","65536"}});add("uw",1,{N8,{"SS_UNMAP","workers"}});add("po",1,{N8,PO});}
 }
 const size_t nSmall=vars.size();
 // final-sized set
 add("Hwarm",1,{N8},"",true);
 for(int r=0;r<3;++r){add("HBs",1,{N8,ST},"",true);add("HB",1,{N8},"",true);}
 if(quick){vars.clear();
  for(int r=0;r<4;++r){
   {const string o=od();pipeline("nd#"+to_string(r),data,o,{N8,ST,{"SS_NODENSE","1"}});error_code ec;fs::remove_all(o,ec);}
   {const string o=od();pipeline("nl#"+to_string(r),data,o,{N8,ST,{"SS_LANES","0"}});error_code ec;fs::remove_all(o,ec);}
   {const string o=od();pipeline("ix#"+to_string(r),data,o,{N8,ST,{"SS_LANES","0"},{"SS_LINES","plain"}});error_code ec;fs::remove_all(o,ec);}
  }}
 pipeline("dev",data,od(),{{"NCPU","8"}});
 (void)nSmall;
 for(int r=0;r<(quick?6:2);++r){
  {const string o=od();pipeline("dv#"+to_string(r),data,o,{N8,ST,{"SS_SMAPS","1"}});error_code ec;fs::remove_all(o,ec);}
  {const string o=od();pipeline("dc#"+to_string(r),devcopy.string(),o,{N8,ST,{"SS_SMAPS","1"}});error_code ec;fs::remove_all(o,ec);}
 }
 // A variant whose label starts with 'O' writes into the real output mount (a subdirectory of it, removed right after),
 // every other one into the scratch mount: the two mounts may not cost the same.
 for(const Var& v:vars){if(!timeleft(v.hugeSet?14:6))break;
  if(v.label[0]=='O'||(v.label[0]=='H'&&v.label.size()>1&&v.label[1]=='O')){const string ro=string(outdir)+"/zz_tmp";pipeline(v.label,v.hugeSet?H:B,ro,v.env,v.bin);error_code ec;fs::remove_all(ro,ec);}
  else{const string o=od();pipeline(v.label,v.hugeSet?H:B,o,v.env,v.bin);error_code ec;fs::remove_all(o,ec);}}
 // Floors: 8 threads over every byte of the 20M replica's event and click files (page cache is warm by now).
 string floorText;
 if(!quick&&timeleft(10)){
  vector<pair<string,off_t>> files;for(const char* sub:{"events","clicks"})for(auto& f:fs::directory_iterator(big/sub)){struct stat st;if(stat(f.path().c_str(),&st)==0)files.push_back({f.path().string(),st.st_size});}
  sort(files.begin(),files.end(),[](auto& x,auto& y){return x.second>y.second;});
  for(int mode=0;mode<3;++mode){double best=1e9;uint64_t tot=0;
   for(int rep=0;rep<3;++rep){atomic<size_t> next{0};atomic<uint64_t> bytes{0},sum{0};
    double t=par(8,[&](int){for(;;){size_t j=next.fetch_add(1);if(j>=files.size())break;int fd=open(files[j].first.c_str(),O_RDONLY);if(fd<0)continue;const off_t n=files[j].second;
      if(n){const char* b=(const char*)mmap(nullptr,n,PROT_READ,MAP_PRIVATE,fd,0);if(b!=MAP_FAILED){madvise((void*)b,n,MADV_HUGEPAGE);uint64_t s2=0;
        if(mode==0){for(off_t k=0;k<n;k+=4096)s2+=(unsigned char)b[k];}
        else if(mode==1){for(off_t k=0;k<n;k+=64)s2+=(unsigned char)b[k];}
        else{const char* q=b;const char* e=b+n;while((q=(const char*)memchr(q,'\n',(size_t)(e-q)))){++s2;++q;}}
        sum+=s2;bytes+=n;munmap((void*)b,n);}}
      close(fd);}});
    if(t<best)best=t;tot=bytes;(void)sum;}
   char one[96];snprintf(one,sizeof one,"%s %.0f ms ",mode==0?"floor: map+touch a byte per 4K":mode==1?"per 64 B":"count newlines (all bytes)",best*1e3);floorText+=one;
   if(mode==2){snprintf(one,sizeof one,"(%.2f GB) ",tot/1e9);floorText+=one;}}
  fprintf(stderr,"DIAG %s\n",floorText.c_str());}
 // Same rows from the dense route and the general route on the final-sized set? (order-independent hash of the debug CSV rows)
 string eqText="eq60 not run";
 if(!quick&&gKeys&&timeleft(40)){
  auto csvhash=[&](const char* name,uint64_t& rows,uint64_t& sum){rows=0;sum=0;const string pth=scratch+"/"+name;int fd=open(pth.c_str(),O_RDONLY);if(fd<0)return false;struct stat st;fstat(fd,&st);
   if(st.st_size){const char* b=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0);uint64_t h=1469598103934665603ull;
    for(off_t k=0;k<st.st_size;++k){if(b[k]=='\n'){h^=h>>29;h*=0xBF58476D1CE4E5B9ull;h^=h>>32;sum+=h;++rows;h=1469598103934665603ull;}else{h^=(unsigned char)b[k];h*=1099511628211ull;}}
    munmap((void*)b,st.st_size);}
   close(fd);unlink(pth.c_str());return true;};
  uint64_t ur[3]={0,0,0},us_[3]={0,0,0},cr[3]={0,0,0},cs[3]={0,0,0};bool ok[3]={false,false,false};
  for(int i=0;i<2;++i){vector<pair<string,string>> env={N8,{"SS_DEBUG_CSV","1"}};if(i==1)env.push_back({"SS_NODENSE","1"});
   {const string o=od();pipeline(i==0?"Hcd":i==1?"Hcn":"Hbd",H,o,env,"");error_code ec;fs::remove_all(o,ec);}ok[i]=csvhash("user_rows.csv",ur[i],us_[i])&&csvhash("campaign_rows.csv",cr[i],cs[i]);}
  auto same=[&](int i){return ok[i]&&ok[1]&&ur[i]==ur[1]&&us_[i]==us_[1]&&ur[i]>0&&cr[i]==cr[1]&&cs[i]==cs[1]&&cr[i]>0;};
  char t[200];snprintf(t,sizeof t,"eq60 general route rows u%llu c%llu: c68 dense %s (u%llu c%llu)",(unsigned long long)ur[1],(unsigned long long)cr[1],same(0)?"SAME":"DIFF",(unsigned long long)ur[0],(unsigned long long)cr[0]);eqText=t;fprintf(stderr,"DIAG %s\n",t);}
 string benchText="bench not run";
 {const string vb=fs::path(core).parent_path().string()+"/vb/vert_bench";
  if(!quick&&fs::exists(vb)&&timeleft(20)){benchText.clear();FILE* pp=popen((vb+" 8 0.25 2>/dev/null").c_str(),"r");if(pp){char line[256];while(fgets(line,sizeof line,pp)){string l=line;while(!l.empty()&&(l.back()=='\n'||l.back()=='\r'))l.pop_back();benchText+=l+"; ";}pclose(pp);}
   fprintf(stderr,"DIAG bench: %s\n",benchText.c_str());}}
 // ---- the real output, then break it on purpose so this entry is rejected and the log is returned ----
 pipeline("final /data -> /output",data,outdir,{{"NCPU","8"}});
 // Put the findings into extra campaign_daily rows: the dev check reports a few rows that differ, as JSON.
 // Row order (by campaign_id) is importance order; the text columns carry the measurements.
 {
  auto us=[](double sec){return (int64_t)(sec*1e6+0.5);};
  auto get=[&](const string& label)->const RunResult*{const RunResult* b=nullptr;for(auto& r:results)if(r.label==label&&r.rc==0&&(!b||r.wall<b->wall))b=&r;return b;};
  auto bestd=[&]()->const RunResult*{const RunResult* b=nullptr;for(auto& r:results)if(r.label.size()==2&&r.label[0]=='d'&&r.rc==0&&(!b||r.wall<b->wall))b=&r;return b;};
  auto stages=[&](const RunResult& r)->string{char t[256];auto g=[&](const char* k){auto it=r.t.find(k);return it==r.t.end()?0.0:it->second*1e3;};
   string s2;for(auto& kv:r.t){char one[48];static const map<string,string> ab={{"AGGREGATE","AGG"},{"DEDUP","DED"},{"DENSE","DNS"},{"DENSE1","DN1"},{"PARSE","PAR"},{"WRITE","WR"},{"FLATTEN","FL"},{"LINKMAP","LM"},{"LINKREAD","LR"},{"LINKSETUP","LS"},{"LINKS","LK"},{"KEYS","KEY"},{"MAP","MAP"}};
    auto it=ab.find(kv.first);snprintf(one,sizeof one,"%s%.1f ",(it==ab.end()?kv.first:it->second).c_str(),kv.second*1e3);s2+=one;}
   snprintf(t,sizeof t,"w%.0f u%.0f s%.0f flt%ld rss%ld cs%ld/%ld | ",r.wall*1e3,r.user*1e3,r.sys*1e3,r.minflt,r.maxrssMB,r.nvcsw,r.nivcsw);(void)g;return string(t)+s2;};
  string cid,chan;vector<int32_t> days;vector<int64_t> c1,c2,c3,c4;int idx=0;
  auto row=[&](const string& label,const string& text,int64_t a,int64_t b,int64_t c,int64_t d){
   char id[16];snprintf(id,sizeof id,"!%02d ",idx++);put_bytes(cid,string(id)+label);put_bytes(chan,text);days.push_back(20000+idx);
   c1.push_back(a);c2.push_back(b);c3.push_back(c);c4.push_back(d);};
  // Smallest value over the repeats of a run group; -1 if the group never ran.
  auto mn=[&](const string& grp,const char* key)->long{double best=-1;for(auto& r:results){if(r.rc!=0||r.label.compare(0,grp.size()+1,grp+"#")!=0)continue;
    double v;const string k=key;
    if(k=="w")v=r.wall;else if(k=="u")v=r.user;else if(k=="s")v=r.sys;else if(k=="f")v=r.minflt/1e3;else if(k=="r")v=r.maxrssMB/1e3;
    else if(k=="L"){v=0;for(auto& kv:r.t)if(kv.first.rfind("LINK",0)==0||kv.first=="FLATTEN")v+=kv.second;}
    else{auto it=r.t.find(k=="P"?"PARSE":k=="D"?"DEDUP":k=="A"?"AGGREGATE":k=="X"?"DENSE":k=="Y"?"DENSE1":k=="K"?"KEYS":k=="W"?"WRITE":k=="R"?"RESOLVE":k=="M"?"MAP":k=="E"?"zEXIT":k=="p"?"cPARSE":k=="q"?"cRESOLVE":k=="x"?"cDENSE":k=="a"?"cAGGREGATE":k=="i"?"sMAP":k=="y"?"sPARSE":k=="h"?"sRESOLVE":k=="z"?"sDENSE":k=="g"?"sAGGREGATE":k=="j"?"fPARSE":k=="k"?"fMAP":k=="e"?"pmd":key);v=it==r.t.end()?0:it->second;}
    if(best<0||v<best)best=v;}
   return best<0?-1:(long)(best*1e3+0.5);};
  auto fmt=[&](const string& name,const string& grp,const char* keys)->string{string o=name+"=";for(const char* k=keys;*k;++k){if(k!=keys)o+="/";const char one[2]={*k,0};o+=to_string(mn(grp,one));}return o+" ";};
  string a,c;
  auto squeeze=[](string x){for(auto pr:{make_pair("serial_pre","sp"),make_pair("first_start","fs"),make_pair("last_end","le"),make_pair("loop_wall","lw"),make_pair("loop_cpu","lc"),make_pair("write_wall","ww"),make_pair("write_max","wm"),make_pair("write_cpu","wc"),make_pair("write_calls","wn"),make_pair("open_wall","ow"),make_pair("grow_wall","gw"),make_pair("grow_calls","gn"),make_pair("tail_wall","tw"),make_pair("end_wall","ew"),make_pair("serial_end","se"),make_pair("thread_cpu","tc"),make_pair("noloop","nl")}){size_t p;while((p=x.find(pr.first))!=string::npos)x.replace(p,strlen(pr.first),pr.second);}
   string y;bool skip=false;size_t i=0;while(i<x.size()){size_t e=x.find(' ',i);if(e==string::npos)e=x.size();const string tok=x.substr(i,e-i);i=e+1;if(tok.empty())continue;
    const bool name=isalpha((unsigned char)tok[0]);if(name)skip=(tok=="n"||tok=="nl"||tok=="se"||tok=="ew"||tok=="wc"||tok=="gn"||tok=="tc"||tok=="le");if(!skip){if(!y.empty())y+=' ';y+=tok;}}
   return y;};
  // median over the repeats of a group, for wall / PARSE / DENSE
  auto med=[&](const string& grp,const char* key)->long{vector<double> v;for(auto& r:results){if(r.rc!=0||r.label.compare(0,grp.size()+1,grp+"#")!=0)continue;
    const string k=key;double x;if(k=="w")x=r.wall;else{auto it=r.t.find(k=="P"?"PARSE":k=="X"?"DENSE":k=="R"?"RESOLVE":k=="A"?"AGGREGATE":key);x=it==r.t.end()?0:it->second;}v.push_back(x);}
   if(v.empty())return -1;sort(v.begin(),v.end());return (long)(v[v.size()/2]*1e3+0.5);};
  auto mm=[&](const string& name,const string& grp)->string{string o=name+"=";const char* ks="wPRXA";for(const char* k=ks;*k;++k){const char one[2]={*k,0};if(k!=ks)o+="/";o+=to_string(mn(grp,one))+"~"+to_string(med(grp,one));}return o+" ";};
  auto mk=[&](const string& name,const string& grp)->string{string o=name+"=";const char* ks="wMKP";for(const char* k=ks;*k;++k){const char one[2]={*k,0};if(k!=ks)o+="/";o+=to_string(mn(grp,one));}return o+"~"+to_string(med(grp,"w"))+" ";};
  {char t[64];snprintf(t,sizeof t," | syncfs after replica %.1fs",syncSec);eqText+=t;}
  {char t[96];snprintf(t,sizeof t," | BOT REPLICA: %zu bots hold %.1f%% of rows",gBots.size(),100.0*gBotRows/1072000.0);eqText+=t;}
  auto allw=[&](const string& name,const string& grp)->string{string o=name+"=";bool first=true;for(auto& r:results){if(r.rc!=0||r.label.compare(0,grp.size()+1,grp+"#")!=0)continue;if(!first)o+=",";first=false;o+=to_string((long)(r.wall*1e3+0.5));}return o+" ";};
  a+=eqText+" | 20M min~median of wall/PARSE/RESOLVE/DENSE/AGG: "+mm("c68","B")+mm("c66","c66")+mm("c68 all four G switches old","gold")+mm("c68 turns old","told")+mm("c68 several old","sold");
  a+="| PUBLIC SET AS MOUNTED (/data) vs A COPY ON /scratch, c68, min of 6: wall/PARSE/sysPARSE ms, PARSE faults (thousands), FilePmdMapped MB, MAP faults: "+fmt("/data","dv","wPyjek")+fmt("copy","dc","wPyjek")+allw("/data walls","dv")+allw("copy walls","dc");
  a+="| small stages MKLDYWE "+fmt("c68","Bs","MKLDYWE")+"cpu PARSE/RESOLVE/DENSE/AGG "+fmt("c68","Bs","pqxa");
  {const RunResult* r=get("Bs#0");if(r)a+="| handed over c68: "+r->hand+" | DENSE threads:"+squeeze(r->dth)+" ";}
  c+="60M min~median w/P/R/X/A: "+mm("c68","HB")+mm("c66","H66")+mm("c68 turns old","Htold")+"cpu PARSE/RESOLVE/DENSE/AGG "+fmt("c68","HBs","pqxa");
  {const RunResult* r=get("HBs#0");if(r)c+="| handed over: "+r->hand+" | DENSE threads c68 60M:"+squeeze(r->dth)+" ";}
  c+="|| "+benchText;
  if(!quick&&gV5){
   char t[160];snprintf(t,sizeof t,"V5 REPLICA (opaque IDs, %zu whale IDs kept = %.1f%% of rows; syncfs %.1fs) GENERAL ROUTE c68 | ",gWhaleIds.size(),100.0*gBotRows/1072000.0,syncSec);
   const char* SK="MKLPDAWE";
   a=string(t)+"20M min~median wall/PARSE/RESOLVE/DENSE/AGG: "+mm("default","B")+mm("personbucket4096","p4")+mm("personbucket65536","p64")+mm("unmap-workers","uw")+mm("4 threads","n4")+mm("16 threads","n16");
   a+="| 20M stages MAP/KEYS/LINKS/PARSE/DEDUP/AGG/WRITE/exit: "+fmt("default","Bs",SK)+"cpu PARSE/AGG "+fmt("","Bs","pa")+"sys MAP/PARSE/AGG "+fmt("","Bs","iyg")+"user/sys total "+fmt("","Bs","us")+"parse-only "+fmt("","po","wPus");
   {const RunResult* r=get("Bs#0");if(r)a+="| "+r->raw.substr(0,420);}
   c="60M min~median w/P/R/X/A: "+mm("default","HB")+"stages MKLPDAWE "+fmt("","HBs",SK)+"cpu PARSE/AGG "+fmt("","HBs","pa")+"user/sys "+fmt("","HBs","us")+allw("walls","HB");
   {const RunResult* r=get("HBs#0");if(r)c+="| "+r->raw.substr(0,420);}
  }
  if(quick){
   a="NEW PUBLIC SET. samples: "+sampleText+" || pipeline (c68) says: "+(results.empty()?string("no run"):results[0].raw);
   {const RunResult* r=get("dv#0");if(r)a+=" || dv: "+r->hand;}
   c="public set, c68, min~median w/P/R/X/A: "+mm("default","dv")+mm("NODENSE","nd")+mm("LANES=0","nl")+mm("plain","ix")+"MKLDYWE "+fmt("default","dv","MKLDYWE")+fmt("nodense","nd","MKLDYWE")+allw("walls default","dv");
   {const RunResult* r=get("nd#0");if(r)c+=" || nodense: "+r->raw.substr(0,330);}
  }
  {const RunResult* d=nullptr;for(auto& r:results)if(r.label.compare(0,2,"B#")==0&&r.rc==0&&(!d||r.wall<d->wall))d=&r;
   row(a,c,d?us(d->wall):0,d?us(d->user):0,d?us(d->sys):0,d?d->minflt:0);}
  // row 1: every run, compact (the checker shows only about 2 KB in all, so this is usually cut)
  {string all;for(auto& r:results){auto g=[&](const char* k){auto it=r.t.find(k);return it==r.t.end()?0.0:it->second*1e3;};char one[96];
    snprintf(one,sizeof one,"%s:%.0f/%.0f/%.0f/%.0f ",r.label.c_str(),r.wall*1e3,g("PARSE"),g("DEDUP"),g("AGGREGATE"));all+=one;}
   row("all runs w/P/D/A",all,0,0,0,0);}
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
