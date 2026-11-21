// Shopping Sessions: exact CSV / identity / attribution core.
// No third-party C++ libraries; the two Parquet tables are written directly.
#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <omp.h>
#include <parallel/algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <cerrno>
#include <cstdio>
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
// Open-addressing string -> uint32 table built for one cache miss per lookup:
// keys of up to 11 bytes (every generated user ID) live inside the 16-byte slot,
// so a probe never follows a pointer. Longer keys fall back to a side list.
// Two modes: a serial one that can grow (links), then a lock-free one with a fixed
// size where many threads look keys up and add the ones nobody has seen.
struct KeyTable{
 struct Slot{atomic<uint64_t> k0;atomic<uint32_t> k1;atomic<uint32_t> v;}; // k1 top byte: 0 empty, len+1 inline, 0xFE being written, 0xFF long (k0=hash, v=index into longs)
 struct Long{string_view key;uint32_t v;};
 static constexpr uint32_t NONE=UINT32_MAX,BUSY=0xFE000000u,LONGK=0xFF000000u;
 Slot* slots=nullptr;size_t cap=0,mask=0,count=0;vector<Long> longs;
 // lock-free mode: new keys get idBase+0, idBase+1, ... up to idLimit keys; newNames keeps their text
 atomic<uint32_t> nextId{0};uint32_t idBase=0,idLimit=0;string_view* newNames=nullptr;mutex longMu;
 explicit KeyTable(size_t want=1024){alloc(want);}
 KeyTable(const KeyTable&)=delete;KeyTable& operator=(const KeyTable&)=delete;
 ~KeyTable(){if(slots)munmap((void*)slots,cap*sizeof(Slot));free(newNames);}
 void alloc(size_t want){
  size_t c=1024;while(c<want)c<<=1;
  void* mem=mmap(nullptr,c*sizeof(Slot),PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0); // zero pages, touched lazily
  if(mem==MAP_FAILED)throw bad_alloc();
  slots=(Slot*)mem;cap=c;mask=c-1;
 }
 static bool pack(string_view s,uint64_t& k0,uint32_t& k1){
  size_t n=s.size();if(n>11)return false;
  k0=0;k1=0;
  if(n>=8){memcpy(&k0,s.data(),8);memcpy(&k1,s.data()+8,n-8);}else memcpy(&k0,s.data(),n);
  k1|=(uint32_t)(n+1)<<24;return true;
 }
 static uint64_t hash_packed(uint64_t k0,uint32_t k1){return mix64(k0^((uint64_t)k1*0xa0761d6478bd642fULL));}
 // ---- serial mode ----
 // Returns the stored value, or stores v and returns it if the key is new (compare with v to tell).
 uint32_t intern(string_view s,uint32_t v){
  if(count*2>=cap)grow();
  const auto R=memory_order_relaxed;
  uint64_t k0;uint32_t k1;
  if(pack(s,k0,k1)){
   for(size_t j=hash_packed(k0,k1)&mask;;j=(j+1)&mask){Slot& e=slots[j];uint32_t c=e.k1.load(R);
    if(c==k1&&e.k0.load(R)==k0)return e.v.load(R);
    if(!c){e.k0.store(k0,R);e.k1.store(k1,R);e.v.store(v,R);++count;return v;}
   }
  }
  uint64_t h=hkey(s);
  for(size_t j=h&mask;;j=(j+1)&mask){Slot& e=slots[j];uint32_t c=e.k1.load(R);
   if(!c){e.k0.store(h,R);e.k1.store(LONGK,R);e.v.store((uint32_t)longs.size(),R);longs.push_back({s,v});++count;return v;}
   if(c==LONGK&&e.k0.load(R)==h&&longs[e.v.load(R)].key==s)return longs[e.v.load(R)].v;
  }
 }
 void grow(size_t want=0){
  const auto R=memory_order_relaxed;
  Slot* old=slots;size_t oldcap=cap;alloc(max(cap*2,want));
  for(size_t i=0;i<oldcap;++i){Slot& e=old[i];uint32_t c=e.k1.load(R);if(!c)continue;
   size_t j=(c==LONGK?e.k0.load(R):hash_packed(e.k0.load(R),c))&mask;
   while(slots[j].k1.load(R))j=(j+1)&mask;
   slots[j].k0.store(e.k0.load(R),R);slots[j].k1.store(c,R);slots[j].v.store(e.v.load(R),R);
  }
  munmap((void*)old,oldcap*sizeof(Slot));
 }
 template<class FN> void remap(FN&& fn){ // rewrite every stored value
  const auto R=memory_order_relaxed;
  for(size_t i=0;i<cap;++i){Slot& e=slots[i];uint32_t c=e.k1.load(R);if(c&&c!=LONGK)e.v.store(fn(e.v.load(R)),R);}
  for(auto& l:longs)l.v=fn(l.v);
 }
 // ---- lock-free mode ----
 void begin_parallel(uint32_t base,uint32_t maxNew){
  idBase=base;idLimit=maxNew;nextId.store(0);
  newNames=(string_view*)calloc((size_t)maxNew+1,sizeof(string_view));if(!newNames)throw bad_alloc();
  if((count+maxNew)*2>cap)grow((count+maxNew)*2);
 }
 uint32_t end_parallel(){uint32_t n=min(nextId.load(),idLimit);count+=n;return n;}
 // The key's value; a key nobody has seen gets the next ID. NONE if the key is new and the table is full.
 uint32_t find_or_add(string_view s){
  const auto R=memory_order_relaxed;
  uint64_t k0;uint32_t k1;
  if(!pack(s,k0,k1))return find_or_add_long(s);
  for(size_t j=hash_packed(k0,k1)&mask;;){
   Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
   if(c==k1){if(e.k0.load(R)==k0)return e.v.load(R);}
   else if(c==0){
    if(nextId.load(R)>=idLimit)return NONE;
    uint32_t expect=0;
    if(!e.k1.compare_exchange_strong(expect,BUSY,memory_order_acquire))continue; // somebody took the slot: look at it again
    uint32_t id=nextId.fetch_add(1,R);
    if(id>=idLimit){e.k1.store(0,memory_order_release);return NONE;}
    newNames[id]=s;e.k0.store(k0,R);e.v.store(idBase+id,R);e.k1.store(k1,memory_order_release);
    return idBase+id;
   }
   else if(c==BUSY){__builtin_ia32_pause();continue;}
   j=(j+1)&mask;
  }
 }
 uint32_t find_or_add_long(string_view s){
  const auto R=memory_order_relaxed;
  lock_guard<mutex> guard(longMu);uint64_t h=hkey(s);
  for(size_t j=h&mask;;){
   Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
   if(c==LONGK){if(e.k0.load(R)==h&&longs[e.v.load(R)].key==s)return longs[e.v.load(R)].v;}
   else if(c==0){
    if(nextId.load(R)>=idLimit)return NONE;
    uint32_t expect=0;
    if(!e.k1.compare_exchange_strong(expect,BUSY,memory_order_acquire))continue;
    uint32_t id=nextId.fetch_add(1,R);
    if(id>=idLimit){e.k1.store(0,memory_order_release);return NONE;}
    newNames[id]=s;longs.push_back({s,idBase+id});
    e.k0.store(h,R);e.v.store((uint32_t)longs.size()-1,R);e.k1.store(LONGK,memory_order_release);
    return idBase+id;
   }
   else if(c==BUSY){__builtin_ia32_pause();continue;}
   j=(j+1)&mask;
  }
 }
};
// Thread-private fallback table (only used if the shared one fills up).
struct SmallTable{
 unordered_map<string_view,uint32_t> m;vector<string_view> keys;
 uint32_t intern(string_view s){auto [it,isnew]=m.try_emplace(s,(uint32_t)keys.size());if(isnew)keys.push_back(s);return it->second;}
};
struct IdGraph{
 KeyTable tab;
 explicit IdGraph(size_t slotsWanted):tab(slotsWanted){}
 vector<uint32_t> parent,sz;
 vector<string_view> names; // at a root: byte-wise smallest ID of the person
 uint32_t intern(string_view s){uint32_t n=(uint32_t)parent.size();
  if(n>=0x7ffffff0u)throw runtime_error("Too many IDs");
  uint32_t id=tab.intern(s,n);
  if(id==n){parent.push_back(n);sz.push_back(1);names.push_back(s);}return id;
 }
 uint32_t find(uint32_t x){uint32_t y=x;while(parent[y]!=y)y=parent[y];while(parent[x]!=x){auto z=parent[x];parent[x]=y;x=z;}return y;}
 void link(string_view a,string_view b){uint32_t x=find(intern(a)),y=find(intern(b));if(x==y)return;
  if(sz[x]<sz[y])swap(x,y);
  parent[y]=x;sz[x]+=sz[y];if(names[y]<names[x])names[x]=names[y];
 }
};
static constexpr uint8_t F_VALID=1,F_PURCHASE=2,F_DEAD=4;
static constexpr uint32_t LOCAL_USER=0x80000000u;
// One parsed event line, bucketed by event_id hash so duplicates meet in one bucket.
struct PRec{const char* id;uint64_t h;int64_t ingest;int64_t ts;uint32_t idlen;uint32_t user;int32_t price;uint16_t cat;uint8_t flags;};
// One valid click line, bucketed by click_id hash.
struct CRec{const char* id;uint64_t h;int64_t ts;uint32_t idlen;uint32_t user;uint16_t campaign;uint8_t flags;};
struct Chunk{const char* base;size_t len,from,to;bool click;};
// One surviving event, 16 bytes, ordered by (person, ts) when compared as (a,b).
// a = person<<32 | tsrel>>8, b = (tsrel&255)<<56 | price<<24 | category<<8 | purchase; tsrel = ts - window_start (< 2^40 ms).
struct Ev{uint64_t a,b;};
static inline bool operator<(const Ev& x,const Ev& y){return x.a!=y.a?x.a<y.a:x.b<y.b;}
struct Click{int64_t ts;uint32_t user;uint16_t campaign;string_view id;};
struct alignas(64) ThreadState{
 SmallTable users; // IDs that did not fit in the shared table
 vector<vector<PRec>> eb;vector<vector<CRec>> cb; // parsed lines by event/click ID hash
 vector<vector<Ev>> uev;vector<vector<Click>> ucl; // survivors by person
};
static size_t pow2_parts(uint64_t est){size_t p=1;while(p<2048&&p*32768<est)p<<=1;return p;}
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
 void row_group(int64_t nrows,const vector<string_view>& bodies){
  buf.clear();Group g;g.rows=nrows;g.bytes=0;
  for(size_t i=0;i<cols.size();++i){
   size_t start=buf.size();
   if(bodies[i].size()>0x7fff0000u)throw runtime_error("Parquet page too large");
   Thrift h(buf);
   h.i32(1,0);h.i32(2,(int64_t)bodies[i].size());h.i32(3,(int64_t)bodies[i].size());
   h.open_struct(5);h.i32(1,nrows);h.i32(2,0);h.i32(3,3);h.i32(4,3);h.close();
   buf.push_back(0);
   buf.append(bodies[i]);
   int64_t size=(int64_t)(buf.size()-start);
   g.cols.push_back({pos+(int64_t)start,size,nrows});g.bytes+=size;
  }
  write_all(buf.data(),buf.size());pos+=(int64_t)buf.size();rows+=nrows;groups.push_back(std::move(g));
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
static void run(const fs::path& data,const fs::path& scratch,const fs::path& output){
 auto __time0=chrono::steady_clock::now();
 auto __mark=[&](const char *label){auto now=chrono::steady_clock::now();cerr<<"TIME "<<label<<" "<<chrono::duration<double>(now-__time0).count()<<"\n";__time0=now;};
 auto manifest=read_all(data/"manifest.json");
 if(!parse_ts(json_string(manifest,"window_start"),window_start)||!parse_ts(json_string(manifest,"window_end"),window_end)||window_end<=window_start)throw runtime_error("Bad manifest");
 int nday=(int)((window_end-window_start+DAY-1)/DAY);
 if(nday<=0||nday>1000)throw runtime_error("Unreasonable window length");
 // The scorer may limit CPU by quota rather than by affinity, so trust NCPU when it is set.
 int T=max(1,omp_get_max_threads());
 if(const char* n=getenv("NCPU")){int v=atoi(n);if(v>=1&&v<=1024)T=v;}
 omp_set_num_threads(T);
 // Map the big inputs up front so the kernel starts reading them while links are processed.
 vector<Chunk> chunks;uint64_t eventBytes=0,clickBytes=0;
 static constexpr size_t CH=size_t(2)<<20;
 auto add_files=[&](const fs::path& dir,bool click,uint64_t& bytes){
  for(const auto& p:csv_files(dir)){Mapped m=map_file(p);if(!m.len)continue;bytes+=m.len;
   for(size_t from=0;from<m.len;from+=CH)chunks.push_back({m.base,m.len,from,from+CH,click});}
 };
 add_files(data/"events",false,eventBytes);
 add_files(data/"clicks",true,clickBytes);
 __mark("MAP");
 // Size the shared ID table once: linked IDs plus room for IDs that only appear in events and clicks.
 uint64_t linkBytes=0;const auto linkFiles=csv_files(data/"links");
 for(const auto& p:linkFiles)linkBytes+=fs::file_size(p);
 uint64_t maxNew64=(eventBytes/70+clickBytes/35)/12+4096;
 if(const char* v=getenv("SS_TEST_MAXNEW"))maxNew64=strtoull(v,nullptr,10); // tests force the fallback path with a tiny value
 if(maxNew64>0x30000000u)maxNew64=0x30000000u;
 const uint32_t maxNew=(uint32_t)maxNew64;
 IdGraph graph((size_t)(linkBytes/8+maxNew)*2);
 for(const auto& p:linkFiles)read_lines(p,[&](string_view line){string_view f[2];if(fields(line,2,f)&&!f[0].empty()&&!f[1].empty())graph.link(f[0],f[1]);});
 __mark("LINKREAD");
 // Flatten: after this the table maps a linked ID straight to its person.
 const uint32_t nlinked=(uint32_t)graph.parent.size();
 {vector<uint32_t> root(nlinked);for(uint32_t i=0;i<nlinked;++i)root[i]=graph.find(i);
  graph.tab.remap([&](uint32_t v){return root[v];});}
 __mark("FLATTEN");
 if((uint64_t)nlinked+maxNew>=0x70000000u)throw runtime_error("Too many IDs");
 graph.tab.begin_parallel(nlinked,maxNew);
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
 const size_t UP=pow2_parts(eventBytes/70); // person buckets for the last stage
 vector<ThreadState> tls(T);
 atomic<size_t> nextChunk{0};
 const int64_t clickStart=window_start-WEEK;
 #pragma omp parallel num_threads(T)
 {
  ThreadState& tl=tls[omp_get_thread_num()];
  tl.eb.resize(PE);tl.cb.resize(PC);tl.uev.resize(UP);tl.ucl.resize(UP);
  {size_t r=(size_t)(eventBytes/70/((uint64_t)T*PE)*5/4)+16;for(auto& v:tl.eb)v.reserve(r);}
  {size_t r=(size_t)(clickBytes/40/((uint64_t)T*PC)*5/4)+16;for(auto& v:tl.cb)v.reserve(r);}
  auto user_of=[&](string_view u)->uint32_t{
   uint32_t g=graph.tab.find_or_add(u);
   if(g!=KeyTable::NONE)return g;
   return LOCAL_USER|tl.users.intern(u);
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
 const uint32_t newCount=graph.tab.end_parallel();
 __mark("PARSE");
 // ---- Fallback only: IDs that did not fit in the shared table get their person number here. ----
 vector<vector<uint32_t>> trans(T);vector<string_view> extraNames;
 const uint32_t extraBase=nlinked+maxNew;
 for(int t=0;t<T;++t){auto& keys=tls[t].users.keys;trans[t].resize(keys.size());
  for(size_t i=0;i<keys.size();++i){
   if(extraNames.size()>=0x08000000u)throw runtime_error("Too many IDs");
   uint32_t n=extraBase+(uint32_t)extraNames.size();
   uint32_t id=graph.tab.intern(keys[i],n);
   if(id==n)extraNames.push_back(keys[i]);
   trans[t][i]=id;
  }
 }
 const string_view* newNames=graph.tab.newNames;
 auto name_of=[&](uint32_t person)->string_view{
  if(person<nlinked)return graph.names[person];
  if(person<extraBase)return newNames[person-nlinked];
  return extraNames[person-extraBase];
 };
 cerr<<"unlinked IDs: "<<newCount<<" (+"<<extraNames.size()<<" via fallback)\n";
 // ---- Dedup each ID bucket on its own; survivors go to person buckets. ----
 auto person_bucket=[UP](uint32_t person)->size_t{return (size_t)(((uint64_t)(person*0x9E3779B1u)*UP)>>32);};
 uint64_t dedupCount=0,surviving=0,survivingClicks=0;
 #pragma omp parallel num_threads(T) reduction(+:dedupCount,surviving,survivingClicks)
 {
  ThreadState& me=tls[omp_get_thread_num()];
  vector<uint64_t> slots;vector<size_t> off(T+1);
  #pragma omp for schedule(dynamic,1) nowait
  for(size_t p=0;p<PE;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=tls[t].eb[p].size();}off[T]=n;
   if(!n)continue;
   size_t cap=16;while(cap<n*2)cap<<=1;slots.assign(cap,0);const size_t mask=cap-1;
   auto at=[&](uint32_t g)->PRec&{int t=0;while(g>=off[t+1])++t;return tls[t].eb[p][g-off[t]];};
   size_t alive=0;
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
   for(int t=0;t<T;++t){auto& v=tls[t].eb[p];
    for(const PRec& r:v)if((r.flags&(F_DEAD|F_VALID))==F_VALID){
     uint32_t u=r.user;if(u&LOCAL_USER)u=trans[t][u&~LOCAL_USER];
     const uint64_t rel=(uint64_t)(r.ts-window_start);
     me.uev[person_bucket(u)].push_back({((uint64_t)u<<32)|(rel>>8),((rel&255)<<56)|((uint64_t)(uint32_t)r.price<<24)|((uint64_t)r.cat<<8)|((r.flags&F_PURCHASE)?1u:0u)});
     ++surviving;
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
   for(int t=0;t<T;++t){auto& v=tls[t].cb[p];
    for(size_t i=0;i<v.size();++i){CRec& r=v[i];const uint32_t g=(uint32_t)(off[t]+i);const uint32_t tag=(uint32_t)(r.h>>18);
     for(size_t j=r.h&mask;;j=(j+1)&mask){
      uint64_t e=slots[j];
      if(!e){slots[j]=((uint64_t)tag<<32)|(g+1);break;}
      if((uint32_t)(e>>32)==tag){const CRec& o=at((uint32_t)e-1);
       if(o.idlen==r.idlen&&memcmp(o.id,r.id,r.idlen)==0){r.flags|=F_DEAD;break;}
      }
     }
    }
   }
   for(int t=0;t<T;++t){auto& v=tls[t].cb[p];
    for(const CRec& r:v)if(!(r.flags&F_DEAD)){
     uint32_t u=r.user;if(u&LOCAL_USER)u=trans[t][u&~LOCAL_USER];
     me.ucl[person_bucket(u)].push_back({r.ts,u,r.campaign,string_view(r.id,r.idlen)});
     ++survivingClicks;
    }
    vector<CRec>().swap(v);
   }
  }
 }
 cerr<<"deduplicated events: "<<dedupCount<<", surviving: "<<surviving<<"\n";
 cerr<<"surviving clicks: "<<survivingClicks<<"\n";
 __mark("DEDUP");
 // ---- Per person bucket: sort, sessionize, attribute. Each thread writes its own Parquet file. ----
 fs::create_directories(output/"user_daily");fs::create_directories(output/"campaign_daily");
 const bool debugCsv=getenv("SS_DEBUG_CSV")!=nullptr; // unit tests read these hex rows from SCRATCH_DIR
 const size_t ncamp=campaigns.size();
 const int64_t firstday=divfloor(window_start,DAY);
 vector<vector<Totals>> threadTotals(T);vector<string> dbg(debugCsv?T:0);
 const vector<PqCol> userCols={{"user_id",PQ_BYTES,false,CONV_UTF8},{"day",PQ_INT32,false,CONV_DATE},{"sessions",PQ_INT64,false,CONV_NONE},
  {"events",PQ_INT64,false,CONV_NONE},{"revenue_cents",PQ_INT64,false,CONV_NONE},{"top_category",PQ_BYTES,true,CONV_UTF8}};
 uint64_t num_sessions=0;
 #pragma omp parallel num_threads(T) reduction(+:num_sessions)
 {
  const int tid=omp_get_thread_num();
  auto& campTotals=threadTotals[tid];campTotals.assign(ncamp*(size_t)nday,Totals{});
  vector<uint32_t> freq(ncamp),positions(ncamp);vector<uint16_t> active;
  vector<uint64_t> catsum(catnames.size());vector<uint16_t> touched;
  auto add_campaign=[&](uint16_t c){if(freq[c]++==0){positions[c]=(uint32_t)active.size();active.push_back(c);}};
  auto remove_campaign=[&](uint16_t c){if(--freq[c]==0){size_t pos=positions[c];uint16_t back=active.back();active[pos]=back;positions[back]=pos;active.pop_back();}};
  PqFile file;
  auto open_file=[&]{char name[32];snprintf(name,sizeof name,"part-%05d.parquet",tid);file.open(output/"user_daily"/name,userCols);};
  string uid,catv,catbody,levels;vector<int32_t> days;vector<int64_t> sess,evs,rev;vector<uint8_t> hascat;
  vector<Ev> events;vector<Click> clicks;
  auto flush_rows=[&]{
   if(days.empty())return;
   // top_category page: 1-bit definition levels as one bit-packed run, then the non-null values.
   const size_t n=days.size(),groups=(n+7)/8;
   levels.clear();put_varint(levels,(groups<<1)|1);size_t at=levels.size();levels.resize(at+groups,0);
   for(size_t i=0;i<n;++i)if(hascat[i])levels[at+i/8]|=(char)(1<<(i%8));
   catbody.clear();put_le32(catbody,(uint32_t)levels.size());catbody+=levels;catbody+=catv;
   if(!file.is_open())open_file();
   file.row_group((int64_t)n,{uid,raw(days),raw(sess),raw(evs),raw(rev),catbody});
   uid.clear();catv.clear();days.clear();sess.clear();evs.clear();rev.clear();hascat.clear();
  };
  #pragma omp for schedule(dynamic,1)
  for(size_t up=0;up<UP;++up){
   events.clear();clicks.clear();
   for(int t=0;t<T;++t){auto& v=tls[t].uev[up];events.insert(events.end(),v.begin(),v.end());vector<Ev>().swap(v);
    auto& c=tls[t].ucl[up];clicks.insert(clicks.end(),c.begin(),c.end());vector<Click>().swap(c);}
   if(events.empty())continue;
   sort(events.begin(),events.end());
   sort(clicks.begin(),clicks.end(),[](const Click& a,const Click& b){
    if(a.user!=b.user)return a.user<b.user;
    if(a.ts!=b.ts)return a.ts<b.ts;
    return a.id>b.id; // last click at equal timestamp has lexicographically smallest ID
   });
   const size_t nE=events.size(),nC=clicks.size();
   auto ts_of=[&](const Ev& e)->int64_t{return window_start+(int64_t)(((e.a&0xffffffffu)<<8)|(e.b>>56));};
   size_t ei=0,cptr=0;
   while(ei<nE){
    const uint32_t person=(uint32_t)(events[ei].a>>32);
    size_t end=ei+1;while(end<nE&&(uint32_t)(events[end].a>>32)==person)++end;
    while(cptr<nC&&clicks[cptr].user<person)++cptr;
    size_t cstart=cptr;while(cptr<nC&&clicks[cptr].user==person)++cptr;
    size_t add=cstart,expire=cstart;
    UserTotals userday;int currentday=-1;
    auto flushday=[&](){
     if(currentday<0)return;
     uint16_t top=0;uint64_t best=0;
     for(uint16_t c:touched){if(catsum[c]>best||(catsum[c]==best&&catsum[c]>0&&catnames[c]<catnames[top])){top=c;best=catsum[c];}catsum[c]=0;}
     touched.clear();
     const string_view name=name_of(person);
     put_bytes(uid,name);days.push_back((int32_t)(firstday+currentday));
     sess.push_back((int64_t)userday.sessions);evs.push_back((int64_t)userday.events);rev.push_back((int64_t)userday.revenue);
     hascat.push_back(best!=0);if(best!=0)put_bytes(catv,catnames[top]);
     if(debugCsv){string& o=dbg[tid];
      o+=hexstr(name);o+=',';o+=to_string(firstday+currentday);o+=',';o+=to_string(userday.sessions);o+=',';o+=to_string(userday.events);o+=',';o+=to_string(userday.revenue);o+=',';
      o+=best==0?string("NULL"):hexstr(catnames[top]);o+='\n';
     }
     userday={};
    };
    for(size_t a=ei;a<end;){
     const int64_t start=ts_of(events[a]);size_t b=a+1;int64_t prev=start;
     while(b<end){int64_t cur=ts_of(events[b]);if(cur-prev>GAP)break;prev=cur;++b;}
     int d=(int)(divfloor(start,DAY)-firstday); // in [0,nday): every surviving event is inside the window
     if(d!=currentday){flushday();currentday=d;}
     uint64_t revenue=0;bool purchasing=false;
     for(size_t k=a;k<b;++k){const Ev& e=events[k];
      if(e.b&1){purchasing=true;const uint64_t price=(e.b>>24)&0xffffffffu;const uint16_t cat=(uint16_t)(e.b>>8);
       revenue+=price;
       if(catsum[cat]==0)touched.push_back(cat);
       catsum[cat]+=price;
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
   if(days.size()>=50000)flush_rows();
  }
  flush_rows();
  if(tid==0&&!file.is_open())open_file(); // the folder always holds at least one file, even with no rows
  if(file.is_open())file.close();
 }
 vector<Totals> campTotals(ncamp*(size_t)nday);
 for(const auto& tt:threadTotals)for(size_t i=0;i<tt.size();++i){campTotals[i].sessions+=tt[i].sessions;campTotals[i].purchasing_sessions+=tt[i].purchasing_sessions;campTotals[i].revenue+=tt[i].revenue;campTotals[i].assists+=tt[i].assists;}
__mark("AGGREGATE");
 {
  string cid,chan,csv;vector<int32_t> days;vector<int64_t> s,p,r,a;
  for(size_t ci=0;ci<ncamp;++ci)for(int d=0;d<nday;++d){const auto& t=campTotals[ci*(size_t)nday+d];if(t.sessions==0&&t.assists==0)continue;
   put_bytes(cid,campaigns[ci].id);put_bytes(chan,campaigns[ci].channel);days.push_back((int32_t)(firstday+d));
   s.push_back((int64_t)t.sessions);p.push_back((int64_t)t.purchasing_sessions);r.push_back((int64_t)t.revenue);a.push_back((int64_t)t.assists);
   if(debugCsv){csv+=hexstr(campaigns[ci].id);csv+=',';csv+=hexstr(campaigns[ci].channel);csv+=',';csv+=to_string(firstday+d);csv+=',';csv+=to_string(t.sessions);csv+=',';csv+=to_string(t.purchasing_sessions);csv+=',';csv+=to_string(t.revenue);csv+=',';csv+=to_string(t.assists);csv+='\n';}
  }
  PqFile f;
  f.open(output/"campaign_daily"/"part-00000.parquet",{{"campaign_id",PQ_BYTES,false,CONV_UTF8},{"channel",PQ_BYTES,false,CONV_UTF8},{"day",PQ_INT32,false,CONV_DATE},
   {"sessions",PQ_INT64,false,CONV_NONE},{"purchasing_sessions",PQ_INT64,false,CONV_NONE},{"revenue_cents",PQ_INT64,false,CONV_NONE},{"assisted_sessions",PQ_INT64,false,CONV_NONE}});
  if(!days.empty())f.row_group((int64_t)days.size(),{cid,chan,raw(days),raw(s),raw(p),raw(r),raw(a)});
  f.close();
  if(debugCsv){
   fs::create_directories(scratch);
   ofstream uout(scratch/"user_rows.csv",ios::binary|ios::trunc),cout(scratch/"campaign_rows.csv",ios::binary|ios::trunc);
   for(const auto& part:dbg)uout<<part;
   cout<<csv;uout.close();cout.close();
   if(!uout||!cout)throw runtime_error("Failed writing debug CSV");
  }
 }
__mark("WRITE"); cerr<<"completed sessions: "<<num_sessions<<"\n";
}
int main(){try{
 const char* d=getenv("DATA_DIR");const char* s=getenv("SCRATCH_DIR");const char* o=getenv("OUTPUT_DIR");
 run(d?d:"/data",s?s:"/scratch",o?o:"/output");return 0;
 }catch(const exception& e){cerr<<"ERROR "<<e.what()<<'\n';return 1;}}
