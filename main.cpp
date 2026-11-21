// Shopping Sessions: exact CSV / identity / attribution core.
// No third-party C++ libraries, intermediate output is hex-encoded to preserve raw identifiers.
#include <algorithm>
#include <atomic>
#include <memory>
#include <omp.h>
#include <parallel/algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;
namespace fs=std::filesystem;
static constexpr int64_t DAY=86400000LL, WEEK=7*DAY, GAP=1800000LL;
static int64_t window_start,window_end;
static bool ascii_digits(string_view s) {if(s.empty())return false;for(unsigned char c:s)if(c<'0'||c>'9')return false;return true;}
static bool dec2(string_view s,int p,int& v){if(p+2>(int)s.size()||s[p]<'0'||s[p]>'9'||s[p+1]<'0'||s[p+1]>'9')return false;v=(s[p]-'0')*10+s[p+1]-'0';return true;}
static bool dec4(string_view s,int p,int& v){if(p+4>(int)s.size())return false;v=0;for(int i=p;i<p+4;i++){if(s[i]<'0'||s[i]>'9')return false;v=v*10+s[i]-'0';}return true;}
static bool leap(int y){return (y%4==0 && y%100!=0)||y%400==0;}
static int64_t days_from_civil(int y,unsigned m,unsigned d){
 y-=m<=2; const int era=(y>=0?y:y-399)/400; const unsigned yoe=(unsigned)(y-era*400);
 const unsigned doy=(153*(m+(m>2?-3:9))+2)/5+d-1;
 const unsigned yoe_days=yoe*365+yoe/4-yoe/100+doy;
 return (int64_t)era*146097+(int)yoe_days-719468;
}
static bool parse_ts(string_view s,int64_t& out){
 if(s.size() && s.size()<=18){
   uint64_t x=0;bool digits=true;
   for(unsigned char c:s){if(c<'0'||c>'9'){digits=false;break;}x=x*10+(c-'0');}
   if(digits){out=(int64_t)x;return true;}
 }else if(ascii_digits(s)){
   uint64_t x=0;
   for(unsigned char c:s){unsigned k=c-'0';if(x>(uint64_t(INT64_MAX)-k)/10)return false;x=x*10+k;}
   out=(int64_t)x;return true;
 }
 if((s.size()!=20&&s.size()!=25)||s[4]!='-'||s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':')return false;
 int y,m,d,h,mi,se,oh=0,om=0;
 if(!dec4(s,0,y)||!dec2(s,5,m)||!dec2(s,8,d)||!dec2(s,11,h)||!dec2(s,14,mi)||!dec2(s,17,se))return false;
 if(m<1||m>12||d<1||h>23||mi>59||se>59)return false;
 int mdays[]={0,31,28+(leap(y)?1:0),31,30,31,30,31,31,30,31,30,31};
 if(d>mdays[m])return false;
 int offset=0;
 if(s.size()==20){if(s[19]!='Z')return false;}
 else {if((s[19]!='+'&&s[19]!='-')||s[22]!=':'||!dec2(s,20,oh)||!dec2(s,23,om)||oh>23||om>59)return false;
   offset=(oh*60+om)*60000;if(s[19]=='-')offset=-offset;
 }
 out=days_from_civil(y,m,d)*DAY+((h*60LL+mi)*60+se)*1000-offset;
 return true;
}
// The challenge's CSV dialect has no embedded separators or quotes, even in quoted fields.
// We explicitly preserve CR as data and never trim whitespace.
static bool fields_slow(string_view line,int n,string_view* out){
 if(line.empty())return false;
 const char *p=line.data(),*end=p+line.size();
 for(int j=0;j<n;j++){
   const char *q=(j==n-1)?end:(const char*)memchr(p,',',end-p);
   if(!q)return false;
   string_view f(p,q-p);
   if(f.size()>=2&&f.front()=='"'&&f.back()=='"'){
     f.remove_prefix(1);f.remove_suffix(1);
     if(f.find('"')!=string_view::npos)return false;
   }else if(f.find('"')!=string_view::npos)return false;
   if(j==n-1&&memchr(p,',',end-p))return false;
   out[j]=f;p=q+1;
 }
 return p==end+1;
}
static bool fields(string_view line,int n,string_view* out){
 if(line.empty())return false;
 const char *p=line.data(),*end=p+line.size();
 if(memchr(p,'"',end-p))return fields_slow(line,n,out);
 for(int j=0;j<n-1;j++){
   const char *q=(const char*)memchr(p,',',end-p);
   if(!q)return false;
   out[j]=string_view(p,q-p);p=q+1;
 }
 if(memchr(p,',',end-p))return false;
 out[n-1]=string_view(p,end-p);
 return true;
}
static vector<fs::path> csv_files(const fs::path& dir){
 vector<fs::path> out;
 if(!fs::is_directory(dir))throw runtime_error("Missing input folder: "+dir.string());
 for(auto& f:fs::directory_iterator(dir))if(f.is_regular_file()&&f.path().extension()==".csv")out.push_back(f.path());
 sort(out.begin(),out.end());return out;
}
// Every input file stays mapped until exit: IDs are kept as views into the maps.
struct Mapped{const char* base=nullptr;size_t len=0;};
static Mapped map_file(const fs::path& path){
 int fd=open(path.c_str(),O_RDONLY);
 if(fd<0)throw runtime_error("Cannot open "+path.string());
 struct stat st{};if(fstat(fd,&st)!=0){close(fd);throw runtime_error("Cannot stat "+path.string());}
 size_t len=st.st_size;
 if(len==0){close(fd);return {};}
 void* base=mmap(nullptr,len,PROT_READ,MAP_PRIVATE,fd,0);
 close(fd);
 if(base==MAP_FAILED)throw runtime_error("Cannot map "+path.string());
 madvise(base,len,MADV_WILLNEED);
 return {(const char*)base,len};
}
// Calls fn for every data line that begins in [from,to). The header line (offset 0) is never passed.
// Only '\n' ends a line; a final line without '\n' still counts.
template<class FN> static void for_lines(const char* base,size_t len,size_t from,size_t to,FN&& fn){
 if(from>=len)return;
 const char* end=base+len;
 const char* nl=(const char*)memchr(from?base+from-1:base,'\n',from?len-(from-1):len);
 if(!nl)return;
 const char* p=nl+1;
 const char* lim=base+min(to,len);
 while(p<lim){
  nl=(const char*)memchr(p,'\n',end-p);
  if(nl){fn(string_view(p,(size_t)(nl-p)));p=nl+1;}
  else{fn(string_view(p,(size_t)(end-p)));break;}
 }
}
template<class FN> static void read_lines(const fs::path& path,FN&& fn){
 Mapped m=map_file(path);if(!m.len)return;
 for_lines(m.base,m.len,0,m.len,fn);
}
static string read_all(const fs::path& path){ifstream in(path,ios::binary);if(!in)throw runtime_error("Cannot open "+path.string());return string(istreambuf_iterator<char>(in),{});}
static string json_string(const string& s,const string& key){
 auto p=s.find('"'+key+'"');if(p==string::npos)throw runtime_error("manifest missing "+key);
 p=s.find(':',p);if(p==string::npos)throw runtime_error("manifest error");
 p=s.find('"',p+1);if(p==string::npos)throw runtime_error("manifest error");
 auto e=s.find('"',p+1);if(e==string::npos)throw runtime_error("manifest error");return s.substr(p+1,e-p-1);
}
static string hexstr(string_view s){static constexpr char h[]="0123456789ABCDEF";string r; r.resize(s.size()*2);for(size_t i=0;i<s.size();i++){unsigned char b=s[i];r[2*i]=h[b>>4];r[2*i+1]=h[b&15];}return r;}
struct TransparentHash {
 using is_transparent=void;
 size_t operator()(string_view x)const noexcept {return hash<string_view>{}(x);}
};
template <class V> using FastMap=unordered_map<string,V,TransparentHash,equal_to<>>;
static uint64_t mix64(uint64_t x){x^=x>>32;x*=0xd6e8feb86659fd93ULL;x^=x>>32;x*=0xd6e8feb86659fd93ULL;return x^(x>>32);}
static uint64_t hkey(string_view s){
 const unsigned char* p=(const unsigned char*)s.data();size_t n=s.size();
 uint64_t h=0x9e3779b97f4a7c15ULL ^ (n*0xbf58476d1ce4e5b9ULL);
 while(n>=8){uint64_t w;memcpy(&w,p,8);h=(h^w)*0xa0761d6478bd642fULL;p+=8;n-=8;}
 uint64_t tail=0;memcpy(&tail,p,n);
 return mix64(h^tail);
}
// Open-addressing string -> dense id table. Keys are views into the mapped inputs.
// find() is const and safe to call from many threads while nobody inserts.
struct StrTable{
 vector<uint64_t> slots; // (hash32<<32)|(id+1), 0 = empty
 vector<string_view> keys;vector<uint32_t> hs;size_t mask=0;
 explicit StrTable(size_t cap=1024){size_t c=1024;while(c<cap)c<<=1;slots.assign(c,0);mask=c-1;}
 size_t size()const{return keys.size();}
 uint32_t find(string_view s,uint32_t h)const{
  for(size_t j=h&mask;;j=(j+1)&mask){
   uint64_t e=slots[j];if(!e)return UINT32_MAX;
   if((uint32_t)(e>>32)==h){const string_view& k=keys[(uint32_t)e-1];if(k.size()==s.size()&&memcmp(k.data(),s.data(),s.size())==0)return (uint32_t)e-1;}
  }
 }
 uint32_t intern(string_view s,uint32_t h){
  size_t j=h&mask;
  for(;;j=(j+1)&mask){
   uint64_t e=slots[j];if(!e)break;
   if((uint32_t)(e>>32)==h){const string_view& k=keys[(uint32_t)e-1];if(k.size()==s.size()&&memcmp(k.data(),s.data(),s.size())==0)return (uint32_t)e-1;}
  }
  if(keys.size()>=0x7ffffff0u)throw runtime_error("Too many IDs");
  uint32_t id=(uint32_t)keys.size();keys.push_back(s);hs.push_back(h);
  slots[j]=((uint64_t)h<<32)|(id+1);
  if(keys.size()*2>slots.size())grow();
  return id;
 }
 void grow(){
  size_t c=slots.size()*2;slots.assign(c,0);mask=c-1;
  for(uint32_t id=0;id<keys.size();++id){size_t j=hs[id]&mask;while(slots[j])j=(j+1)&mask;slots[j]=((uint64_t)hs[id]<<32)|(id+1);}
 }
};
struct IdGraph{
 StrTable tab{1<<16};
 vector<uint32_t> parent,sz;
 vector<string_view> names; // at a root: byte-wise smallest ID of the person
 uint32_t intern(string_view s){uint32_t n=(uint32_t)tab.size();uint32_t id=tab.intern(s,(uint32_t)hkey(s));
  if(id==n){parent.push_back(n);sz.push_back(1);names.push_back(s);}return id;
 }
 uint32_t find(uint32_t x){uint32_t y=x;while(parent[y]!=y)y=parent[y];while(parent[x]!=x){auto z=parent[x];parent[x]=y;x=z;}return y;}
 void link(string_view a,string_view b){uint32_t x=find(intern(a)),y=find(intern(b));if(x==y)return;
  if(sz[x]<sz[y])swap(x,y);parent[y]=x;sz[x]+=sz[y];if(names[y]<names[x])names[x]=names[y];
 }
};
static constexpr uint8_t F_VALID=1,F_PURCHASE=2,F_DEAD=4;
static constexpr uint32_t LOCAL_USER=0x80000000u;
// One parsed event line, bucketed by event_id hash so duplicates meet in one bucket.
struct PRec{const char* id;uint64_t h;int64_t ingest;int64_t ts;uint32_t idlen;uint32_t user;int32_t price;uint16_t cat;uint8_t flags;};
// One valid click line, bucketed by click_id hash.
struct CRec{const char* id;uint64_t h;int64_t ts;uint32_t idlen;uint32_t user;uint16_t campaign;uint8_t flags;};
struct Chunk{const char* base;size_t len,from,to;bool click;};
struct alignas(64) ThreadState{
 StrTable users{1<<12}; // IDs seen in events/clicks that no link mentions
 vector<vector<PRec>> eb;vector<vector<CRec>> cb;
};
template<class T> struct Arr{ // uninitialised buffer, so pages are first touched by the threads that fill it
 unique_ptr<T[]> p;size_t n=0;
 void alloc(size_t k){p.reset(new T[k]);n=k;}
 size_t size()const{return n;}T& operator[](size_t i){return p[i];}const T& operator[](size_t i)const{return p[i];}
 T* begin(){return p.get();}T* end(){return p.get()+n;}
};
static size_t pow2_parts(uint64_t est){size_t p=1;while(p<2048&&p*32768<est)p<<=1;return p;}
struct Event{int64_t ts;uint32_t user;int32_t price;uint16_t cat;bool purchase;};
struct Click{int64_t ts;uint32_t user;uint16_t campaign;string_view id;};
struct Camp{string id,channel;};
struct Totals{uint64_t sessions=0,purchasing_sessions=0,revenue=0,assists=0;};
struct UserTotals{uint64_t sessions=0,events=0,revenue=0;};
static int64_t divfloor(int64_t a,int64_t b){int64_t q=a/b;return q-((a%b)<0);}
static bool price_value(string_view s,int32_t& val){if(!ascii_digits(s))return false;uint64_t p=0;for(char c:s){p=p*10+(c-'0');if(p>10000000)return false;}if(!p)return false;val=p;return true;}
static uint16_t cat_id(string_view cat,FastMap<uint16_t>& ids,vector<string>& names){
 auto it=ids.find(cat);if(it!=ids.end())return it->second;
 if(names.size()>=65535)throw runtime_error("Too many categories");
 uint16_t n=names.size();names.emplace_back(cat);ids.emplace(names.back(),n);return n;
}
static void run(const fs::path& data,const fs::path& scratch){
 auto __time0=chrono::steady_clock::now();
 auto __mark=[&](const char *label){auto now=chrono::steady_clock::now();cerr<<"TIME "<<label<<" "<<chrono::duration<double>(now-__time0).count()<<"\n";__time0=now;};
 auto manifest=read_all(data/"manifest.json");
 if(!parse_ts(json_string(manifest,"window_start"),window_start)||!parse_ts(json_string(manifest,"window_end"),window_end)||window_end<=window_start)throw runtime_error("Bad manifest");
 int nday=(int)((window_end-window_start+DAY-1)/DAY);
 if(nday<=0||nday>1000)throw runtime_error("Unreasonable window length");
 const int T=max(1,omp_get_max_threads());
 // Map the big inputs up front so the kernel starts reading them while links are processed.
 vector<Chunk> chunks;uint64_t eventBytes=0,clickBytes=0;
 static constexpr size_t CH=size_t(2)<<20;
 auto add_files=[&](const fs::path& dir,bool click,uint64_t& bytes){
  for(const auto& p:csv_files(dir)){Mapped m=map_file(p);if(!m.len)continue;bytes+=m.len;
   for(size_t from=0;from<m.len;from+=CH)chunks.push_back({m.base,m.len,from,from+CH,click});}
 };
 add_files(data/"events",false,eventBytes);
 add_files(data/"clicks",true,clickBytes);
 IdGraph graph;
 for(const auto& p:csv_files(data/"links"))read_lines(p,[&](string_view line){string_view f[2];if(fields(line,2,f)&&!f[0].empty()&&!f[1].empty())graph.link(f[0],f[1]);});
 // Flatten: root[id] is the person of a linked ID. Read-only from here on.
 const uint32_t nlinked=(uint32_t)graph.parent.size();
 vector<uint32_t> root(nlinked);
 for(uint32_t i=0;i<nlinked;++i)root[i]=graph.find(i);
 __mark("LINKS"); cerr<<"linked IDs: "<<nlinked<<"\n";
 FastMap<uint16_t> categories;vector<string> catnames;
 cat_id("unknown",categories,catnames);
 FastMap<uint16_t> products;
 read_lines(data/"products.csv",[&](string_view line){string_view f[2];if(!fields(line,2,f))throw runtime_error("Malformed clean products.csv");products.emplace(string(f[0]),cat_id(f[1],categories,catnames));});
 vector<Camp> campaigns;FastMap<uint16_t> camps;
 read_lines(data/"campaigns.csv",[&](string_view line){string_view f[2];if(!fields(line,2,f))throw runtime_error("Malformed clean campaigns.csv");if(campaigns.size()>=65534)throw runtime_error("Too many campaigns");camps.emplace(string(f[0]),(uint16_t)campaigns.size());campaigns.push_back({string(f[0]),string(f[1])});});
 uint16_t organic=(uint16_t)campaigns.size();campaigns.push_back({"organic","organic"});

 // ---- Parse every event and click line in parallel, bucketing by ID hash. ----
 const size_t PE=pow2_parts(eventBytes/70),PC=pow2_parts(clickBytes/40);
 vector<ThreadState> tls(T);
 atomic<size_t> nextChunk{0};
 const int64_t clickStart=window_start-WEEK;
 #pragma omp parallel num_threads(T)
 {
  ThreadState& tl=tls[omp_get_thread_num()];
  tl.eb.resize(PE);tl.cb.resize(PC);
  {size_t r=(size_t)(eventBytes/70/((uint64_t)T*PE)*5/4)+16;for(auto& v:tl.eb)v.reserve(r);}
  {size_t r=(size_t)(clickBytes/40/((uint64_t)T*PC)*5/4)+16;for(auto& v:tl.cb)v.reserve(r);}
  auto user_of=[&](string_view u)->uint32_t{
   uint32_t h=(uint32_t)hkey(u);uint32_t g=graph.tab.find(u,h);
   if(g!=UINT32_MAX)return root[g];
   return LOCAL_USER|tl.users.intern(u,h);
  };
  auto on_event=[&](string_view line){
   string_view f[7];if(!fields(line,7,f)||f[0].empty())return;
   int64_t ingest=0;if(!parse_ts(f[3],ingest))return; // ingest is guaranteed to be valid F1
   PRec r;r.id=f[0].data();r.idlen=(uint32_t)f[0].size();r.h=hkey(f[0]);r.ingest=ingest;r.ts=0;r.user=0;r.price=0;r.cat=0;r.flags=0;
   int64_t ts;
   if(!f[1].empty()&&!f[5].empty()&&parse_ts(f[2],ts)&&ts>=window_start&&ts<window_end&&ingest-ts<=DAY&&
     (f[4]=="view"||f[4]=="cart"||f[4]=="purchase")){
    int32_t price=0;bool purchase=f[4]=="purchase";
    if(!purchase||price_value(f[6],price)){
     r.flags=F_VALID|(purchase?F_PURCHASE:0);r.ts=ts;r.price=price;r.user=user_of(f[1]);
     if(purchase){auto prod=products.find(f[5]);r.cat=prod==products.end()?0:prod->second;}
    }
   }
   tl.eb[(size_t)(((r.h>>40)*PE)>>24)].push_back(r);
  };
  auto on_click=[&](string_view line){
   string_view f[4];if(!fields(line,4,f)||f[0].empty())return;
   // Copies of a click are identical, so validating before the dedup keeps the same clicks.
   int64_t ts=0;
   if(f[1].empty()||!parse_ts(f[3],ts)||ts<clickStart||ts>=window_end)return;
   auto ci=camps.find(f[2]);if(ci==camps.end())return;
   CRec r;r.id=f[0].data();r.idlen=(uint32_t)f[0].size();r.h=hkey(f[0]);r.ts=ts;r.user=user_of(f[1]);r.campaign=ci->second;r.flags=0;
   tl.cb[(size_t)(((r.h>>40)*PC)>>24)].push_back(r);
  };
  for(;;){
   size_t c=nextChunk.fetch_add(1,memory_order_relaxed);if(c>=chunks.size())break;
   const Chunk& ck=chunks[c];
   if(ck.click)for_lines(ck.base,ck.len,ck.from,ck.to,on_click);
   else for_lines(ck.base,ck.len,ck.from,ck.to,on_event);
  }
 }
 __mark("PARSE");
 // ---- Give IDs that no link mentions a global person number. ----
 vector<vector<uint32_t>> trans(T);
 for(int t=0;t<T;++t){auto& u=tls[t].users;trans[t].resize(u.size());
  for(uint32_t i=0;i<u.size();++i){uint32_t n=(uint32_t)graph.tab.size();uint32_t id=graph.tab.intern(u.keys[i],u.hs[i]);
   if(id==n)graph.names.push_back(u.keys[i]);
   trans[t][i]=id; // not linked, so the ID is its own person
  }
 }
 __mark("USER_MERGE");
 // ---- Dedup each bucket on its own. ----
 vector<vector<Event>> eouts(PE);vector<vector<Click>> couts(PC);
 uint64_t dedupCount=0;
 #pragma omp parallel num_threads(T) reduction(+:dedupCount)
 {
  vector<uint64_t> slots;vector<size_t> off(T+1);
  #pragma omp for schedule(dynamic,1) nowait
  for(size_t p=0;p<PE;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=tls[t].eb[p].size();}off[T]=n;
   if(!n)continue;
   size_t cap=16;while(cap<n*2)cap<<=1;slots.assign(cap,0);const size_t mask=cap-1;
   auto at=[&](uint32_t g)->PRec&{int t=0;while(g>=off[t+1])++t;return tls[t].eb[p][g-off[t]];};
   size_t alive=0,valid=0;
   for(int t=0;t<T;++t){auto& v=tls[t].eb[p];
    for(size_t i=0;i<v.size();++i){PRec& r=v[i];const uint32_t g=(uint32_t)(off[t]+i);const uint32_t tag=(uint32_t)(r.h>>18);
     for(size_t j=r.h&mask;;j=(j+1)&mask){
      uint64_t e=slots[j];
      if(!e){slots[j]=((uint64_t)tag<<32)|(g+1);++alive;break;}
      if((uint32_t)(e>>32)==tag){PRec& o=at((uint32_t)e-1);
       if(o.idlen==r.idlen&&memcmp(o.id,r.id,r.idlen)==0){
        // Keep the earliest ingested copy; copies with equal ingest_ts are identical.
        if(r.ingest<o.ingest){o.flags|=F_DEAD;slots[j]=((uint64_t)tag<<32)|(g+1);}else r.flags|=F_DEAD;
        break;
       }
      }
     }
    }
   }
   for(int t=0;t<T;++t)for(const PRec& r:tls[t].eb[p])valid+=(r.flags&(F_DEAD|F_VALID))==F_VALID;
   auto& out=eouts[p];out.reserve(valid);
   for(int t=0;t<T;++t){auto& v=tls[t].eb[p];
    for(const PRec& r:v)if((r.flags&(F_DEAD|F_VALID))==F_VALID){
     uint32_t u=r.user;if(u&LOCAL_USER)u=trans[t][u&~LOCAL_USER];
     out.push_back({r.ts,u,r.price,r.cat,(r.flags&F_PURCHASE)!=0});
    }
    vector<PRec>().swap(v);
   }
   dedupCount+=alive;
  }
  #pragma omp for schedule(dynamic,1)
  for(size_t p=0;p<PC;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=tls[t].cb[p].size();}off[T]=n;
   if(!n)continue;
   size_t cap=16;while(cap<n*2)cap<<=1;slots.assign(cap,0);const size_t mask=cap-1;
   auto at=[&](uint32_t g)->CRec&{int t=0;while(g>=off[t+1])++t;return tls[t].cb[p][g-off[t]];};
   size_t alive=0;
   for(int t=0;t<T;++t){auto& v=tls[t].cb[p];
    for(size_t i=0;i<v.size();++i){CRec& r=v[i];const uint32_t g=(uint32_t)(off[t]+i);const uint32_t tag=(uint32_t)(r.h>>18);
     for(size_t j=r.h&mask;;j=(j+1)&mask){
      uint64_t e=slots[j];
      if(!e){slots[j]=((uint64_t)tag<<32)|(g+1);++alive;break;}
      if((uint32_t)(e>>32)==tag){const CRec& o=at((uint32_t)e-1);
       if(o.idlen==r.idlen&&memcmp(o.id,r.id,r.idlen)==0){r.flags|=F_DEAD;break;}
      }
     }
    }
   }
   auto& out=couts[p];out.reserve(alive);
   for(int t=0;t<T;++t){auto& v=tls[t].cb[p];
    for(const CRec& r:v)if(!(r.flags&F_DEAD)){
     uint32_t u=r.user;if(u&LOCAL_USER)u=trans[t][u&~LOCAL_USER];
     out.push_back({r.ts,u,r.campaign,string_view(r.id,r.idlen)});
    }
    vector<CRec>().swap(v);
   }
  }
 }
 Arr<Event> events;Arr<Click> clicks;
 {
  vector<size_t> eoff(PE+1,0),coff(PC+1,0);
  for(size_t p=0;p<PE;++p)eoff[p+1]=eoff[p]+eouts[p].size();
  for(size_t p=0;p<PC;++p)coff[p+1]=coff[p]+couts[p].size();
  events.alloc(eoff[PE]);clicks.alloc(coff[PC]);
  #pragma omp parallel num_threads(T)
  {
   #pragma omp for schedule(dynamic,8) nowait
   for(size_t p=0;p<PE;++p){if(!eouts[p].empty())memcpy(&events[eoff[p]],eouts[p].data(),eouts[p].size()*sizeof(Event));vector<Event>().swap(eouts[p]);}
   #pragma omp for schedule(dynamic,8)
   for(size_t p=0;p<PC;++p){copy(couts[p].begin(),couts[p].end(),clicks.begin()+coff[p]);vector<Click>().swap(couts[p]);}
  }
 }
 cerr<<"deduplicated events: "<<dedupCount<<", surviving: "<<events.size()<<"\n";
 cerr<<"surviving clicks: "<<clicks.size()<<"\n";
 __mark("DEDUP");
 __gnu_parallel::sort(events.begin(),events.end(),[](const Event& a,const Event& b){if(a.user!=b.user)return a.user<b.user;return a.ts<b.ts;});
 __gnu_parallel::sort(clicks.begin(),clicks.end(),[](const Click& a,const Click& b){
  if(a.user!=b.user)return a.user<b.user;if(a.ts!=b.ts)return a.ts<b.ts;
  return a.id>b.id; // last click at equal timestamp has lexicographically smallest ID
 });
__mark("SORT"); fs::create_directories(scratch);
 ofstream uout(scratch/"user_rows.csv",ios::binary|ios::trunc);
 ofstream cout(scratch/"campaign_rows.csv",ios::binary|ios::trunc);
 if(!uout||!cout)throw runtime_error("Cannot open scratch files");
 vector<Totals> campTotals(campaigns.size()*(size_t)nday);
 vector<uint32_t> freq(campaigns.size());vector<uint32_t> positions(campaigns.size());vector<uint16_t> active;
 vector<uint64_t> catsum(catnames.size());vector<uint16_t> touched;
 int64_t firstday=divfloor(window_start,DAY);
 auto day_index=[&](int64_t ts)->int{return int(divfloor(ts,DAY)-firstday);};
 auto add_campaign=[&](uint16_t c){if(freq[c]++==0){positions[c]=(uint32_t)active.size();active.push_back(c);}};
 auto remove_campaign=[&](uint16_t c){if(--freq[c]==0){size_t pos=positions[c];uint16_t back=active.back();active[pos]=back;positions[back]=pos;active.pop_back();}};
 size_t cptr=0;size_t num_sessions=0;
 for(size_t ei=0;ei<events.size();){
  uint32_t person=events[ei].user;
  size_t end=ei+1;while(end<events.size()&&events[end].user==person)++end;
  while(cptr<clicks.size()&&clicks[cptr].user<person)++cptr;
  size_t cstart=cptr;while(cptr<clicks.size()&&clicks[cptr].user==person)++cptr;
  size_t add=cstart,expire=cstart;
  UserTotals userday;int currentday=-1;
  auto flushday=[&](){
   if(currentday<0)return;
   uint16_t top=0;uint64_t best=0;
   for(uint16_t c:touched){if(catsum[c]>best||(catsum[c]==best&&catsum[c]>0&&catnames[c]<catnames[top])){top=c;best=catsum[c];}catsum[c]=0;}
   touched.clear();
   uout<<hexstr(graph.names[person])<<','<<(firstday+currentday)<<','<<userday.sessions<<','<<userday.events<<','<<userday.revenue<<',';
   if(best==0)uout<<"NULL";else uout<<hexstr(catnames[top]);
   uout<<'\n';userday={};
  };
  for(size_t a=ei;a<end;){
   int64_t start=events[a].ts;size_t b=a+1;
   while(b<end&&events[b].ts-events[b-1].ts<=GAP)++b;
   int d=day_index(start);if(d<0||d>=nday)throw runtime_error("Session day outside manifest");
   if(d!=currentday){flushday();currentday=d;}
   uint64_t revenue=0;bool purchasing=false;
   for(size_t k=a;k<b;++k){auto& e=events[k];
    if(e.purchase){purchasing=true;revenue+=e.price;
      if(catsum[e.cat]==0)touched.push_back(e.cat);
      catsum[e.cat]+=e.price;
    }
   }
   userday.sessions++;userday.events+=b-a;userday.revenue+=revenue;
   // Sliding seven-day window, inclusive on both ends.
   while(add<cptr&&clicks[add].ts<=start){add_campaign(clicks[add].campaign);++add;}
   while(expire<add&&clicks[expire].ts<start-WEEK){remove_campaign(clicks[expire].campaign);++expire;}
   uint16_t credited=organic;
   if(add>expire)credited=clicks[add-1].campaign;
   auto& t=campTotals[(size_t)credited*nday+d];t.sessions++;t.purchasing_sessions+=purchasing;t.revenue+=revenue;
   for(uint16_t ci:active)campTotals[(size_t)ci*nday+d].assists++;
   num_sessions++;a=b;
  }
  flushday();
  for(uint16_t ci:active)freq[ci]=0;
  active.clear();ei=end;
 }
__mark("AGGREGATE"); for(size_t ci=0;ci<campaigns.size();++ci){
  for(int d=0;d<nday;++d){auto& t=campTotals[ci*(size_t)nday+d];if(t.sessions==0&&t.assists==0)continue;
   cout<<hexstr(campaigns[ci].id)<<','<<hexstr(campaigns[ci].channel)<<','<<(firstday+d)<<','<<t.sessions<<','<<t.purchasing_sessions<<','<<t.revenue<<','<<t.assists<<'\n';
  }
 }
 uout.close();cout.close();
 if(!uout||!cout)throw runtime_error("Failed writing intermediate CSV");
__mark("WRITE"); cerr<<"completed sessions: "<<num_sessions<<"\n";
}
int main(){try{
 const char* d=getenv("DATA_DIR");const char* s=getenv("SCRATCH_DIR");
 run(d?d:"/data",s?s:"/scratch");return 0;
 }catch(const exception& e){cerr<<"ERROR "<<e.what()<<'\n';return 1;}}