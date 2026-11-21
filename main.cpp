// Shopping Sessions: exact CSV / identity / attribution core.
// No third-party C++ libraries; the two Parquet tables are written directly.
#include <algorithm>
#include <atomic>
#include <bit>
#include <memory>
#include <mutex>
#include <thread>
#include <omp.h>
#include <parallel/algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <climits>
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
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <immintrin.h>
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
// Eight ASCII digits to a number without a loop (little-endian SWAR). False if any byte is not 0-9.
static inline bool parse8(const char* p,uint64_t& v){
 uint64_t x;memcpy(&x,p,8);
 if(((x&0xF0F0F0F0F0F0F0F0ULL)|(((x+0x0606060606060606ULL)&0xF0F0F0F0F0F0F0F0ULL)>>4))!=0x3333333333333333ULL)return false;
 x=((x&0x0F0F0F0F0F0F0F0FULL)*2561)>>8;
 x=((x&0x00FF00FF00FF00FFULL)*6553601)>>16;
 x=((x&0x0000FFFF0000FFFFULL)*42949672960001ULL)>>32;
 v=x;return true;
}
// "YYYY-MM-DD" -> milliseconds at 00:00 UTC, remembered per thread: a window holds few distinct dates,
// so almost every lookup is two compares. Only valid dates are remembered.
struct DateMemo{uint64_t k8;uint16_t k2;int64_t ms;};
static thread_local DateMemo dateMemo[64];
static inline bool f1_date(const char* p,int64_t& dayms){
 uint64_t k8;uint16_t k2;memcpy(&k8,p,8);memcpy(&k2,p+8,2);
 DateMemo& e=dateMemo[(((uint32_t)k2*2654435761u)^((uint32_t)(k8>>32)*40503u))>>26];
 if(e.k8==k8&&e.k2==k2&&k8){dayms=e.ms;return true;} // k8 is never 0 for a real date; the k8 test keeps an empty entry from matching NUL bytes
 if(p[4]!='-'||p[7]!='-')return false;
 static constexpr unsigned char pos[8]={0,1,2,3,5,6,8,9};
 unsigned d[8];for(int i=0;i<8;++i){d[i]=(unsigned)(unsigned char)p[pos[i]]-'0';if(d[i]>9)return false;}
 const int y=(int)(d[0]*1000+d[1]*100+d[2]*10+d[3]),m=(int)(d[4]*10+d[5]),dd=(int)(d[6]*10+d[7]);
 static constexpr int mlen[13]={0,31,28,31,30,31,30,31,31,30,31,30,31};
 if(m<1||m>12||dd<1||dd>mlen[m]+((m==2&&leap(y))?1:0))return false;
 dayms=days_from_civil(y,(unsigned)m,(unsigned)dd)*DAY;
 e.k8=k8;e.k2=k2;e.ms=dayms;return true;
}
// Time of day of an F1/F2 timestamp ("....-..-..THH:MM:SS"): true and milliseconds since midnight if
// the 'T', both colons and all six digits are in place and in range. The date and zone are not looked at.
static inline bool hms_ms(const char* p,int64_t& ms){
 uint64_t t;memcpy(&t,p+11,8);
 const bool shape=(p[10]=='T')&((t&0xF0F0FFF0F0FFF0F0ULL)==0x30303A30303A3030ULL)
  &(((t+0x0606000606000606ULL)&0xF0F000F0F000F0F0ULL)==0x3030003030003030ULL);
 if(!shape)return false;
 const unsigned h=(unsigned)((t&15)*10+((t>>8)&15)),mi=(unsigned)(((t>>24)&15)*10+((t>>32)&15)),se=(unsigned)(((t>>48)&15)*10+((t>>56)&15));
 if(h>23||mi>59||se>59)return false;
 ms=((h*60LL+mi)*60+se)*1000;return true;
}
// Same result as parse_ts. The common shapes (F1, F2 and 13-digit F3) take a short path;
// anything they do not recognise as well-formed goes to parse_ts, which decides.
static inline bool parse_ts_fast(string_view s,int64_t& out){
 const char* p=s.data();
 if(s.size()==20){
  int64_t dayms,ms;
  if(p[19]=='Z'&&hms_ms(p,ms)&&f1_date(p,dayms)){out=dayms+ms;return true;}
 }else if(s.size()==13){
  uint64_t hi,lo; // digits 0-7 and 5-12; together they cover and check all thirteen
  if(parse8(p,hi)&&parse8(p+5,lo)){out=(int64_t)(hi*100000ULL+lo%100000ULL);return true;}
 }else if(s.size()==25){
  int64_t dayms,ms;
  const unsigned o1=(unsigned)(unsigned char)p[20]-'0',o2=(unsigned)(unsigned char)p[21]-'0',o3=(unsigned)(unsigned char)p[23]-'0',o4=(unsigned)(unsigned char)p[24]-'0';
  if((p[19]=='+'||p[19]=='-')&&p[22]==':'&&o1<=9&&o2<=9&&o3<=9&&o4<=9&&hms_ms(p,ms)&&f1_date(p,dayms)){
   const int oh=(int)(o1*10+o2),om=(int)(o3*10+o4);
   if(oh>23||om>59)return false;
   const int64_t off=(oh*60+om)*60000LL;
   out=dayms+ms-(p[19]=='-'?-off:off);
   return true;
  }
 }
 return parse_ts(s,out);
}
// 1 view, 2 cart, 3 purchase, 0 anything else (exact, case-sensitive).
static inline unsigned event_kind(string_view t){
 if(t.size()==4){uint32_t w;memcpy(&w,t.data(),4);return w==0x77656976u?1u:(w==0x74726163u?2u:0u);}
 if(t.size()==8){uint64_t w;memcpy(&w,t.data(),8);return w==0x6573616863727570ULL?3u:0u;}
 return 0;
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
[[maybe_unused]] static bool fields(string_view line,int n,string_view* out){ // reference splitter; the stages use scan_line + take_fields
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
 // Reserve one extra zero page after the file so word and vector loads may run a little past
 // the last byte (bytes after EOF read as zero). The file is mapped over the start of the reservation.
 const size_t maplen=(len+4095)&~size_t(4095);
 void* base=mmap(nullptr,maplen+4096,PROT_READ,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
 if(base==MAP_FAILED){close(fd);throw runtime_error("Cannot reserve "+path.string());}
 void* got=mmap(base,len,PROT_READ,MAP_PRIVATE|MAP_FIXED,fd,0);
 close(fd);
 if(got==MAP_FAILED)throw runtime_error("Cannot map "+path.string());
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
// One pass over a line with SSE2: finds its end and records where the commas are and how many quotes it has.
// Returns the start of the next line. Never reads past `end`.
static inline const char* scan_line(const char* p,const char* end,const char** comma,int maxc,int& ncomma,int& nquote,const char*& lineEnd){
 ncomma=0;nquote=0;const char* q=p;
 const __m128i vnl=_mm_set1_epi8('\n'),vc=_mm_set1_epi8(','),vq=_mm_set1_epi8('"');
 while(end-q>=16){
  const __m128i x=_mm_loadu_si128((const __m128i*)q);
  unsigned mnl=(unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(x,vnl));
  unsigned mc=(unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(x,vc));
  unsigned mq=(unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(x,vq));
  if(mnl){const unsigned cut=(unsigned)__builtin_ctz(mnl),keep=(1u<<cut)-1;mc&=keep;mq&=keep;
   while(mq){++nquote;mq&=mq-1;}
   while(mc){if(ncomma<maxc)comma[ncomma]=q+__builtin_ctz(mc);++ncomma;mc&=mc-1;}
   lineEnd=q+cut;return q+cut+1;
  }
  while(mq){++nquote;mq&=mq-1;}
  while(mc){if(ncomma<maxc)comma[ncomma]=q+__builtin_ctz(mc);++ncomma;mc&=mc-1;}
  q+=16;
 }
 for(;q<end;++q){const char ch=*q;
  if(ch=='\n'){lineEnd=q;return q+1;}
  if(ch==','){if(ncomma<maxc)comma[ncomma]=q;++ncomma;}else if(ch=='"')++nquote;
 }
 lineEnd=end;return end;
}
// Splits a scanned line into exactly n fields under the track's CSV dialect: a quote may only wrap a whole field.
static inline bool take_fields(const char* p,const char* lineEnd,const char* const* comma,int ncomma,int nquote,int n,string_view* out){
 if(ncomma!=n-1||p==lineEnd)return false;
 int quoted=0;const char* s=p;
 for(int j=0;j<n;++j){const char* e=j==n-1?lineEnd:comma[j];
  if(e>s&&*s=='"'){if(e-s<2||e[-1]!='"')return false;out[j]=string_view(s+1,(size_t)(e-s-2));++quoted;}
  else out[j]=string_view(s,(size_t)(e-s));
  s=e+1;
 }
 return nquote==2*quoted; // any other quote sits inside a field, which is malformed
}
// Like for_lines, but hands fn the split fields of every well-formed N-field line that begins in [from,to).
// Lines are handled in small batches: pre(fields) runs for the whole batch first so it can prefetch
// the memory fn(fields) is going to touch.
template<int N,class PRE,class FN> static void for_records(const char* base,size_t len,size_t from,size_t to,PRE&& pre,FN&& fn){
 if(from>=len)return;
 const char* end=base+len;
 const char* nl=(const char*)memchr(from?base+from-1:base,'\n',from?len-(from-1):len);
 if(!nl)return;
 const char* p=nl+1;
 const char* lim=base+min(to,len);
 static constexpr int BATCH=12;
 const char* comma[N];string_view f[BATCH][N];
 while(p<lim){
  int nb=0;
  while(nb<BATCH&&p<lim){
   int ncomma,nquote;const char* lineEnd;
   const char* next=scan_line(p,end,comma,N,ncomma,nquote,lineEnd);
   if(take_fields(p,lineEnd,comma,ncomma,nquote,N,f[nb])){pre(f[nb]);++nb;}
   p=next;
  }
  for(int i=0;i<nb;++i)fn(f[i]);
 }
}
#define SS_V3 __attribute__((target("avx2,bmi,bmi2,popcnt,lzcnt")))
template<class FN> static void read_lines(const fs::path& path,FN&& fn){
 Mapped m=map_file(path);if(!m.len)return;
 for_lines(m.base,m.len,0,m.len,fn);
}
// Calls fn(fields, ok) for every data line of a two-field CSV file; ok is false for a line that is not
// exactly two well-formed fields (the fields are then unspecified). Same line and dialect rules as for_lines + fields.
template<class FN> static void read_pairs(const fs::path& path,FN&& fn){
 Mapped m=map_file(path);if(!m.len)return;
 const char* end=m.base+m.len;
 const char* nl=(const char*)memchr(m.base,'\n',m.len);
 if(!nl)return;
 for(const char* p=nl+1;p<end;){
  const char* comma[2];int ncomma,nquote;const char* lineEnd;string_view f[2];
  const char* next=scan_line(p,end,comma,2,ncomma,nquote,lineEnd);
  fn(f,take_fields(p,lineEnd,comma,ncomma,nquote,2,f));
  p=next;
 }
}
// Number of '\n' bytes in [p,p+n).
static size_t count_newlines(const char* p,size_t n){
 const __m128i nl=_mm_set1_epi8('\n'),zero=_mm_setzero_si128();
 __m128i total=zero;
 while(n>=16){
  const size_t blk=min<size_t>(n/16,255); // each byte lane can count 255 matches before it is summed
  __m128i acc=zero;
  for(size_t i=0;i<blk;++i){acc=_mm_sub_epi8(acc,_mm_cmpeq_epi8(_mm_loadu_si128((const __m128i*)p),nl));p+=16;}
  total=_mm_add_epi64(total,_mm_sad_epu8(acc,zero));n-=blk*16;
 }
 size_t c=(size_t)_mm_cvtsi128_si64(total)+(size_t)_mm_cvtsi128_si64(_mm_unpackhi_epi64(total,total));
 for(;n;--n)c+=*p++=='\n';
 return c;
}
static string read_all(const fs::path& path){ifstream in(path,ios::binary);if(!in)throw runtime_error("Cannot open "+path.string());return string(istreambuf_iterator<char>(in),{});}
static string json_string(const string& s,const string& key){
 auto p=s.find('"'+key+'"');if(p==string::npos)throw runtime_error("manifest missing "+key);
 p=s.find(':',p);if(p==string::npos)throw runtime_error("manifest error");
 p=s.find('"',p+1);if(p==string::npos)throw runtime_error("manifest error");
 auto e=s.find('"',p+1);if(e==string::npos)throw runtime_error("manifest error");return s.substr(p+1,e-p-1);
}
static string hexstr(string_view s){static constexpr char h[]="0123456789ABCDEF";string r; r.resize(s.size()*2);for(size_t i=0;i<s.size();i++){unsigned char b=s[i];r[2*i]=h[b>>4];r[2*i+1]=h[b&15];}return r;}
// Switches for measuring on the target machine; all default to the fast setting.
static bool optHuge=true,optPrefault=true,optAsyncFree=true;
struct TransparentHash {
 using is_transparent=void;
 size_t operator()(string_view x)const noexcept {return hash<string_view>{}(x);}
};
template <class V> using FastMap=unordered_map<string,V,TransparentHash,equal_to<>>;
static uint64_t mix64(uint64_t x){x^=x>>32;x*=0xd6e8feb86659fd93ULL;x^=x>>32;x*=0xd6e8feb86659fd93ULL;return x^(x>>32);}
// Low n bytes of a little-endian word. Views into the mapped inputs may be read up to 16 bytes past
// their end (see map_file), so short strings are loaded whole and masked instead of copied byte by byte.
static constexpr uint64_t LOW_BYTES[9]={0,0xFFULL,0xFFFFULL,0xFFFFFFULL,0xFFFFFFFFULL,0xFFFFFFFFFFULL,0xFFFFFFFFFFFFULL,0xFFFFFFFFFFFFFFULL,~0ULL};
static inline uint64_t hkey(string_view s){ // s must be a view into a mapped input
 const unsigned char* p=(const unsigned char*)s.data();size_t n=s.size();
 uint64_t h=0x9e3779b97f4a7c15ULL ^ (n*0xbf58476d1ce4e5b9ULL);
 while(n>=8){uint64_t w;memcpy(&w,p,8);h=(h^w)*0xa0761d6478bd642fULL;p+=8;n-=8;}
 uint64_t tail;memcpy(&tail,p,8);tail&=LOW_BYTES[n];
 return mix64(h^tail);
}
static atomic<bool> anyLongName{false}; // a person name longer than 15 bytes still points into an input file
// An ID's text in 16 bytes: up to 15 bytes are stored in place (one cache miss to read a name),
// longer ones keep a pointer into the mapped input.
struct PName{
 char b[16];
 void set(string_view s){
  if(s.size()<=15){memset(b,0,16);memcpy(b,s.data(),s.size());b[15]=(char)s.size();}
  else{const char* p=s.data();uint32_t n=(uint32_t)s.size();memcpy(b,&p,8);memcpy(b+8,&n,4);b[15]=(char)0xFF;anyLongName.store(true,memory_order_relaxed);}
 }
 // Same as set() for a key of at most 11 bytes in the table's packed form (k1's top byte is length+1).
 void set_packed(uint64_t k0,uint32_t k1){
  const uint64_t hi=(uint64_t)(k1&0x00ffffffu)|((uint64_t)((k1>>24)-1)<<56);
  memcpy(b,&k0,8);memcpy(b+8,&hi,8);
 }
 string_view view()const{
  if((unsigned char)b[15]!=0xFF)return string_view(b,(size_t)(unsigned char)b[15]);
  const char* p;uint32_t n;memcpy(&p,b,8);memcpy(&n,b+8,4);return string_view(p,n);
 }
};
// Byte-wise order of two names. Names stored in place are zero-padded with the length in the last byte,
// so comparing the 16 bytes as one big-endian number gives the same answer as comparing the strings.
static inline bool name_less(const PName& a,const PName& b){
 if((unsigned char)a.b[15]!=0xFF&&(unsigned char)b.b[15]!=0xFF){
  uint64_t ah,al,bh,bl;memcpy(&ah,a.b,8);memcpy(&al,a.b+8,8);memcpy(&bh,b.b,8);memcpy(&bl,b.b+8,8);
  if(ah!=bh)return __builtin_bswap64(ah)<__builtin_bswap64(bh);
  return __builtin_bswap64(al)<__builtin_bswap64(bl);
 }
 return a.view()<b.view();
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
 atomic<uint32_t> nextId{0};uint32_t idBase=0,idLimit=0;PName* newNames=nullptr;size_t namesBytes=0;mutex longMu;
 explicit KeyTable(size_t want=1024){alloc(want);}
 KeyTable(const KeyTable&)=delete;KeyTable& operator=(const KeyTable&)=delete;
 ~KeyTable(){if(slots)munmap((void*)slots,cap*sizeof(Slot));if(newNames)munmap((void*)newNames,namesBytes);}
 void alloc(size_t want){
  size_t c=1024;while(c<want)c<<=1;
  void* mem=mmap(nullptr,c*sizeof(Slot),PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0); // zero pages, touched lazily
  if(mem==MAP_FAILED)throw bad_alloc();
  if(optHuge)madvise(mem,c*sizeof(Slot),MADV_HUGEPAGE); // fewer page faults and TLB misses where the kernel allows it
  slots=(Slot*)mem;cap=c;mask=c-1;
 }
 __attribute__((always_inline)) static bool pack(string_view s,uint64_t& k0,uint32_t& k1){ // s must be a view into a mapped input
  const size_t n=s.size();if(n>11)return false;
  uint64_t lo;uint32_t hi;memcpy(&lo,s.data(),8);memcpy(&hi,s.data()+8,4);
  if(n<8){k0=lo&LOW_BYTES[n];k1=0;}else{k0=lo;k1=hi&(uint32_t)LOW_BYTES[n-8];}
  k1|=(uint32_t)(n+1)<<24;return true;
 }
 __attribute__((always_inline)) static uint64_t hash_packed(uint64_t k0,uint32_t k1){return mix64(k0^((uint64_t)k1*0xa0761d6478bd642fULL));}
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
  if(!count){munmap((void*)old,oldcap*sizeof(Slot));return;} // nothing to move: do not even touch the old pages
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
  // Straight from mmap: calloc would hand out heap memory and zero all of it here, on one thread.
  namesBytes=((size_t)maxNew+1)*sizeof(PName);
  void* mem=mmap(nullptr,namesBytes,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
  if(mem==MAP_FAILED)throw bad_alloc();
  newNames=(PName*)mem;
  if((count+maxNew)*2>cap)grow((count+maxNew)*2);
 }
 uint32_t end_parallel(){uint32_t n=min(nextId.load(),idLimit);count+=n;return n;}
 // Lookup only (no insert). Safe from many threads as long as nobody is adding keys.
 // Lookup of a key that is already packed (k0,k1 from pack()).
 __attribute__((always_inline)) uint32_t find_packed(uint64_t k0,uint32_t k1)const{
  const auto R=memory_order_relaxed;
  for(size_t j=hash_packed(k0,k1)&mask;;j=(j+1)&mask){const Slot& e=slots[j];const uint32_t c=e.k1.load(R);
   if(c==k1&&e.k0.load(R)==k0)return e.v.load(R);
   if(!c)return NONE;
  }
 }
 // Replaces the value of a key that is in the table (packed form). Other threads may do the same for other keys.
 void set_packed(uint64_t k0,uint32_t k1,uint32_t v){
  const auto R=memory_order_relaxed;
  for(size_t j=hash_packed(k0,k1)&mask;;j=(j+1)&mask){Slot& e=slots[j];const uint32_t c=e.k1.load(R);
   if(c==k1&&e.k0.load(R)==k0){e.v.store(v,R);return;}
   if(!c)return;
  }
 }
 uint32_t find(string_view s)const{
  const auto R=memory_order_relaxed;
  uint64_t k0;uint32_t k1;
  if(pack(s,k0,k1))return find_packed(k0,k1);
  const uint64_t h=hkey(s);
  for(size_t j=h&mask;;j=(j+1)&mask){const Slot& e=slots[j];const uint32_t c=e.k1.load(R);
   if(!c)return NONE;
   if(c==LONGK&&e.k0.load(R)==h&&longs[e.v.load(R)].key==s)return longs[e.v.load(R)].v;
  }
 }
 void prefetch(string_view s)const{uint64_t k0;uint32_t k1;if(pack(s,k0,k1))__builtin_prefetch(&slots[hash_packed(k0,k1)&mask],0,1);}
 // The key's value; a key nobody has seen gets the next ID. NONE if the key is new and the table is full.
 uint32_t find_or_add(string_view s){
  uint64_t k0;uint32_t k1;
  if(!pack(s,k0,k1))return find_or_add_long(s);
  return find_or_add_packed(k0,k1,hash_packed(k0,k1)&mask,s);
 }
 // Same, for a key that is already packed (k0,k1 from pack(s)) with j = hash_packed(k0,k1)&mask.
 uint32_t find_or_add_packed(uint64_t k0,uint32_t k1,size_t j,string_view){
  const auto R=memory_order_relaxed;
  for(;;){
   Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
   if(c==k1){if(e.k0.load(R)==k0)return e.v.load(R);}
   else if(c==0){
    if(nextId.load(R)>=idLimit)return NONE;
    uint32_t expect=0;
    if(!e.k1.compare_exchange_strong(expect,BUSY,memory_order_acquire))continue; // somebody took the slot: look at it again
    uint32_t id=nextId.fetch_add(1,R);
    if(id>=idLimit){e.k1.store(0,memory_order_release);return NONE;}
    newNames[id].set_packed(k0,k1);e.k0.store(k0,R);e.v.store(idBase+id,R);e.k1.store(k1,memory_order_release);
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
    newNames[id].set(s);longs.push_back({s,idBase+id});
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
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
// Bump allocator for the big per-thread bucket buffers. Regions come straight from mmap, ask for
// huge pages, and can be pre-faulted in one kernel call, so the hot loops do not take a page fault
// per 4 KB of output. Nothing is handed back piecemeal: a whole stage's regions are unmapped at once.
struct Region{void* base;size_t len;};
static mutex regionMu;static vector<Region> stageRegions[2]; // [0] parsed-line buckets, [1] person buckets
struct Arena{
 char* cur=nullptr;char* lim=nullptr;int stage=0;
 void add_region(size_t bytes,size_t warm){
  const size_t huge=size_t(2)<<20;
  const size_t want=(max<size_t>(bytes,size_t(32)<<20)+huge-1)&~(huge-1);
  void* mem=mmap(nullptr,want+huge,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
  if(mem==MAP_FAILED)throw bad_alloc();
  char* a=(char*)(((uintptr_t)mem+huge-1)&~(uintptr_t)(huge-1)); // 2 MB aligned so huge pages can be used
  if(optHuge)madvise(a,want,MADV_HUGEPAGE);
  if(warm&&optPrefault)madvise(a,min(warm,want),MADV_POPULATE_WRITE); // ignored by old kernels: pages then fault in one by one
  cur=a;lim=a+want;
  lock_guard<mutex> g(regionMu);stageRegions[stage].push_back({mem,want+huge});
 }
 void* get(size_t bytes){
  bytes=(bytes+63)&~size_t(63);
  if((size_t)(lim-cur)<bytes)add_region(bytes,0);
  void* r=cur;cur+=bytes;return r;
 }
};
static thread_local Arena tlArena;
template<class T> struct ArenaAlloc{
 using value_type=T;
 ArenaAlloc()=default;
 template<class U> ArenaAlloc(const ArenaAlloc<U>&){}
 T* allocate(size_t n){return (T*)tlArena.get(n*sizeof(T));}
 void deallocate(T*,size_t){}
 template<class U> bool operator==(const ArenaAlloc<U>&)const{return true;}
 template<class U> bool operator!=(const ArenaAlloc<U>&)const{return false;}
};
template<class T> using AVec=vector<T,ArenaAlloc<T>>;
static void unmap_regions(vector<Region> regions){for(const Region& r:regions)munmap(r.base,r.len);}
static constexpr uint8_t F_VALID=1,F_PURCHASE=2,F_DEAD=4;
static constexpr uint32_t LOCAL_USER=0x80000000u;
// One parsed event line, bucketed by event_id hash so duplicates meet in one bucket.
struct PRec{const char* id;uint64_t h;int64_t ingest;int64_t ts;uint32_t idlen;uint32_t user;int32_t price;uint16_t cat;uint8_t flags;};
// One valid click line, bucketed by click_id hash.
struct CRec{const char* id;uint64_t h;int64_t ts;uint32_t idlen;uint32_t user;uint16_t campaign;uint8_t flags;};
struct Chunk{const char* base;size_t len,from,to;bool click;};
// One surviving event, 16 bytes. k = tsrel<<25 | purchase<<24 | price, with tsrel = ts - window_start
// (< 2^37 ms: the window is at most 1000 days) and price <= 10,000,000 < 2^24. person is the compact person number.
struct Ev{uint64_t k;uint32_t person;uint16_t cat;uint16_t pad;};
static constexpr uint64_t EV_PURCHASE=1ull<<24,EV_PRICE=EV_PURCHASE-1;
static constexpr unsigned EV_TS=25;
static_assert(((uint64_t)F_PURCHASE<<23)==EV_PURCHASE); // the flag bit is shifted straight into place
// One surviving click, 24 bytes. key = (ts - (window_start - 7 days))<<16 | campaign.
struct Click{uint64_t key;const char* id;uint32_t person;uint32_t idlen;};
// Order of a person's clicks: by ts, and at equal ts the byte-wise larger ID first, so the last click
// at or before a moment is the latest one with the smallest ID.
static inline bool click_before(const Click& a,const Click& b){
 const uint64_t x=a.key>>16,y=b.key>>16;
 if(x!=y)return x<y;
 return string_view(a.id,a.idlen)>string_view(b.id,b.idlen);
}
static inline void click_insertion_sort(Click* c0,Click* c1){
 for(Click* x=c0+1;x<c1;++x){
  if(click_before(*x,x[-1])){const Click v=*x;Click* y=x;do{*y=y[-1];--y;}while(y>c0&&click_before(v,y[-1]));*y=v;}
 }
}
// Grow-only scratch array that is never zero-filled.
template<class T> struct Scratch{
 T* p=nullptr;size_t cap=0;
 Scratch()=default;Scratch(const Scratch&)=delete;Scratch& operator=(const Scratch&)=delete;
 ~Scratch(){free(p);}
 T* get(size_t n){if(n>cap){free(p);cap=n+n/4+64;p=(T*)malloc(cap*sizeof(T));if(!p)throw bad_alloc();}return p;}
};
// Memory for the block lists below: handed out from 4 MB slabs owned by one thread, given back all at once.
// The slabs come straight from mmap (2 MB aligned, huge pages where allowed) like the bucket arenas.
struct SlabPool{
 char* cur=nullptr;char* end=nullptr;vector<Region> slabs;
 SlabPool()=default;SlabPool(const SlabPool&)=delete;SlabPool& operator=(const SlabPool&)=delete;
 ~SlabPool(){release();}
 void release(){for(const Region& r:slabs)munmap(r.base,r.len);slabs.clear();cur=end=nullptr;}
 __attribute__((noinline,cold)) void* take(size_t bytes){
  if((size_t)(end-cur)<bytes){
   static constexpr size_t SLAB=size_t(4)<<20,HUGE=size_t(2)<<20;
   void* mem=mmap(nullptr,SLAB+HUGE,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
   if(mem==MAP_FAILED){fprintf(stderr,"ERROR out of memory\n");_exit(1);}
   char* a=(char*)(((uintptr_t)mem+HUGE-1)&~(uintptr_t)(HUGE-1));
   if(optHuge)madvise(a,SLAB,MADV_HUGEPAGE);
   slabs.push_back({mem,SLAB+HUGE});cur=a;end=a+SLAB;
  }
  void* r=cur;cur+=bytes;return r;
 }
};
// Append-only list of fixed-size blocks: nothing is ever copied when it grows, and an element's address stays valid.
template<class T,size_t BYTES> struct Blocks{
 static constexpr size_t PER=(BYTES-16)/sizeof(T);
 struct Block{Block* next;size_t pad;T items[PER];};
 T* cur=nullptr;T* end=nullptr;Block* head=nullptr;Block* tail=nullptr;size_t nblocks=0;
 __attribute__((noinline,cold)) void grow(SlabPool& pool){
  Block* b=(Block*)pool.take(sizeof(Block));
  b->next=nullptr;if(tail)tail->next=b;else head=b;
  tail=b;++nblocks;cur=b->items;end=b->items+PER;
 }
 T* slot(SlabPool& pool){if(cur==end)grow(pool);return cur++;} // the place for the next element
 size_t size()const{return nblocks?(nblocks-1)*PER+(size_t)(cur-tail->items):0;}
 size_t count(const Block* b)const{return b==tail?(size_t)(cur-b->items):PER;}
};
using EvList=Blocks<Ev,4096>;using ClickList=Blocks<Click,4096>;
struct alignas(64) ThreadState{
 SmallTable users; // IDs that did not fit in the shared table
 vector<AVec<PRec>> eb;vector<AVec<CRec>> cb; // parsed lines by event/click ID hash
 vector<AVec<Ev>> uev;vector<AVec<Click>> ucl; // survivors by person
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
// ---- AVX2 parse path (CPUs with AVX2+BMI, checked once at start-up) ----
// A line is cut straight from comma/quote/newline bit masks and parsed with its fields held in
// registers; nothing is stored per field. Any line the short path is not sure about (too long for
// its windows, malformed, an unusual ingest_ts, a user ID too long for an in-slot key, the last
// line of a file without '\n') is handed to the portable splitter and the general handler, which decide.
// Valid lines are finished in two steps: the record is pushed with user 0 and the table slot of its
// user is prefetched; once a small batch has collected, the users are looked up in line order.
#define SS_V3I SS_V3 static inline __attribute__((always_inline))
SS_V3I uint64_t mask64_v3(__m256i a,__m256i b,__m256i v){
 return (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a,v))|((uint64_t)(uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b,v))<<32);
}
// Quote rules for one window of a line, as bit masks: q = quotes, starts = first byte of each field,
// ends = the comma or newline that ends each field (q has no bit 63). `opened` gets the end bit of every
// field that starts with a quote: adding the opening quotes to ~ends carries from each one up to its
// field's end bit. A quote that does not open a field must close one, i.e. sit right before an end bit,
// so shifting those up by one must give exactly `opened`. The result is zero iff every quote wraps a
// whole field ("" included; a lone " or a quote inside a field is not zero).
SS_V3I uint64_t quote_errors_v3(uint64_t q,uint64_t starts,uint64_t ends,uint64_t& sq,uint64_t& opened){
 sq=q&starts;
 opened=(sq+~ends)&ends;
 return opened^((q&~starts)<<1);
}
// Time of day of "....-..-..THH:MM:SS": milliseconds since midnight, or -1 unless the 'T', both colons
// and the six digits are in place and in range. The date and what follows the seconds are not looked at.
SS_V3I int64_t tod_v3(const char* p){
 const __m128i x=_mm_loadu_si128((const __m128i*)(p+3)); // bytes 3..18: 7 date bytes, then THH:MM:SS
 const __m128i tmpl=_mm_setr_epi8(0,0,0,0,0,0,0,'T','0','0',':','0','0',':','0','0');
 const __m128i lim=_mm_setr_epi8(-1,-1,-1,-1,-1,-1,-1,0,2,9,0,5,9,0,5,9);
 const __m128i v=_mm_sub_epi8(x,tmpl);
 if(_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_max_epu8(v,lim),lim))!=0xFFFF)return -1;
 const __m128i w8=_mm_setr_epi8(0,0,0,0,0,0,0,0,10,1,0,10,1,0,10,1);
 const __m128i w16=_mm_setr_epi16(0,0,0,0,3600,60,60,1);
 const __m128i s=_mm_madd_epi16(_mm_maddubs_epi16(v,w8),w16); // dword 2 = H*3600+tens of minutes*600, dword 3 = minute units*60+S
 const uint64_t r=(uint64_t)_mm_extract_epi64(s,1);
 const uint64_t ms=(r*((1000ULL<<32)|1000ULL))>>32;   // (dword2+dword3)*1000; both products stay below 2^32
 return ms<86400000ULL?(int64_t)ms:-1;                 // hours 24..29 pass the per-digit limits
}
// Thirteen ASCII digits (epoch milliseconds) to a number; false if any of them is not 0-9.
SS_V3I bool epoch13_v3(const char* p,int64_t& out){
 const __m128i v=_mm_sub_epi8(_mm_loadu_si128((const __m128i*)p),_mm_set1_epi8('0'));
 const __m128i lim=_mm_setr_epi8(9,9,9,9,9,9,9,9,9,9,9,9,9,-1,-1,-1);
 if(_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_max_epu8(v,lim),lim))!=0xFFFF)return false;
 const __m128i a=_mm_maddubs_epi16(v,_mm_setr_epi8(10,1,10,1,10,1,10,1,10,1,10,1,1,0,0,0)); // six 2-digit numbers, then digit 12
 const __m128i b=_mm_madd_epi16(a,_mm_setr_epi16(100,1,100,1,100,1,1,0));                    // three 4-digit numbers, then digit 12
 const __m128i c=_mm_packus_epi32(b,b);
 const __m128i e=_mm_madd_epi16(c,_mm_setr_epi16(10000,1,10,1,0,0,0,0));                     // digits 0-7 and digits 8-12
 const uint64_t r=(uint64_t)_mm_cvtsi128_si64(e);
 out=(int64_t)((r&0xFFFFFFFFULL)*100000ULL+(r>>32));
 return true;
}
// "YYYY-MM-DD" -> milliseconds at 00:00 UTC, remembered per thread. The slot comes straight from the
// month and day digits, so the dates of one year never push each other out. Only valid dates are stored.
struct DateMemo3{uint64_t k;uint32_t cc;uint32_t pad;int64_t ms;}; // k = bytes 2..9, cc = bytes 0..1; cc of an unused entry is all ones and never matches
static thread_local DateMemo3 dateMemo3[4096];
static thread_local bool dateMemo3Ready=false;
static DateMemo3* date_memo3(){
 if(!dateMemo3Ready){for(auto& e:dateMemo3){e.k=0;e.cc=0xFFFFFFFFu;e.pad=0;e.ms=0;}dateMemo3Ready=true;}
 return dateMemo3;
}
[[gnu::noinline]] static bool date3_miss(const char* p,DateMemo3& e,int64_t& dayms){
 if(p[4]!='-'||p[7]!='-')return false;
 static constexpr unsigned char pos[8]={0,1,2,3,5,6,8,9};
 unsigned d[8];for(int i=0;i<8;++i){d[i]=(unsigned)(unsigned char)p[pos[i]]-'0';if(d[i]>9)return false;}
 const int y=(int)(d[0]*1000+d[1]*100+d[2]*10+d[3]),m=(int)(d[4]*10+d[5]),dd=(int)(d[6]*10+d[7]);
 static constexpr int mlen[13]={0,31,28,31,30,31,30,31,31,30,31,30,31};
 if(m<1||m>12||dd<1||dd>mlen[m]+((m==2&&leap(y))?1:0))return false;
 dayms=days_from_civil(y,(unsigned)m,(unsigned)dd)*DAY;
 uint16_t cc;memcpy(&e.k,p+2,8);memcpy(&cc,p,2);e.cc=cc;e.ms=dayms;return true;
}
static inline __attribute__((always_inline)) bool date3(const char* p,DateMemo3* memo,int64_t& dayms){
 uint64_t k;uint16_t cc;memcpy(&k,p+2,8);memcpy(&cc,p,2);
 // gathers the low bits of the month and day digits into a 12-bit slot number
 DateMemo3& e=memo[((k&0x0F03000F01000000ULL)*((1ULL<<4)|(1ULL<<22)|(1ULL<<34)))>>52];
 if(__builtin_expect(e.k==k&&e.cc==cc,1)){dayms=e.ms;return true;}
 return date3_miss(p,e,dayms);
}
// Same result as parse_ts for the n bytes at p (which may be read a little past their end).
SS_V3I bool ts_v3(const char* p,unsigned n,DateMemo3* memo,int64_t& out){
 if(n==20){
  int64_t tod,dayms;
  if(p[19]=='Z'&&(tod=tod_v3(p))>=0&&date3(p,memo,dayms)){out=dayms+tod;return true;}
 }else if(n==13){
  if(epoch13_v3(p,out))return true;
 }else if(n==25){
  int64_t tod,dayms;
  const unsigned o1=(unsigned)(unsigned char)p[20]-'0',o2=(unsigned)(unsigned char)p[21]-'0',o3=(unsigned)(unsigned char)p[23]-'0',o4=(unsigned)(unsigned char)p[24]-'0';
  if((p[19]=='+'||p[19]=='-')&&p[22]==':'&&o1<=9&&o2<=9&&o3<=9&&o4<=9&&(tod=tod_v3(p))>=0&&date3(p,memo,dayms)){
   const int oh=(int)(o1*10+o2),om=(int)(o3*10+o4);
   if(oh>23||om>59)return false;
   const int64_t off=(oh*60+om)*60000LL;
   out=dayms+tod-(p[19]=='-'?-off:off);
   return true;
  }
 }
 return parse_ts(string_view(p,n),out);
}
// hkey() of the n bytes at p, without the loop for the usual short IDs.
// HKEY_START[n] is hkey's starting value for length n.
static constexpr array<uint64_t,16> HKEY_START=[]{array<uint64_t,16> a{};for(uint64_t n=0;n<16;++n)a[n]=0x9e3779b97f4a7c15ULL^(n*0xbf58476d1ce4e5b9ULL);return a;}();
SS_V3I uint64_t hkey_v3(const char* p,size_t n){
 if(__builtin_expect(n>=16,0))return hkey(string_view(p,n));
 uint64_t h=HKEY_START[n],w0,w1,tail;
 memcpy(&w0,p,8);memcpy(&w1,p+8,8);
 if(n>=8){h=(h^w0)*0xa0761d6478bd642fULL;tail=_bzhi_u64(w1,(unsigned)(8*(n-8)));}
 else tail=_bzhi_u64(w0,(unsigned)(8*n));
 return mix64(h^tail);
}
// A pushed record that still needs its user: the packed user key, its table slot, and the record.
// A bucket is only ever grown right after the waiting records have been finished (see the capacity
// checks), so `rec` cannot be left pointing into storage that has moved.
template<class R> struct PendUser{uint64_t k0;const char* u;R* rec;uint32_t j;uint32_t k1;}; // j fits: the table has at most 2^32 slots
static constexpr int V3_BATCH=16;
template<class R> static void resolve_users(const PendUser<R>* pend,const PendUser<R>* pendEnd,KeyTable& tab,ThreadState& tl){
 const auto R_=memory_order_relaxed;
 KeyTable::Slot* const slots=tab.slots;
 for(;pend!=pendEnd;++pend){
  const PendUser<R>& q=*pend;KeyTable::Slot& e=slots[q.j];uint32_t g;
  if(__builtin_expect(e.k1.load(memory_order_acquire)==q.k1&&e.k0.load(R_)==q.k0,1))g=e.v.load(R_);
  else{
   const string_view u(q.u,(size_t)(q.k1>>24)-1);
   g=tab.find_or_add_packed(q.k0,q.k1,q.j,u);
   if(g==KeyTable::NONE)g=LOCAL_USER|tl.users.intern(u);
  }
  q.rec->user=g;
 }
}
// Does for every data line that begins in [from,to) exactly what for_records<7> + the general event
// handler would do; `general` is that handler and gets the lines the short path passes on.
template<class GEN> SS_V3 static void events_v3(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){
 if(from>=len)return;
 const char* end=base+len;
 const char* nl=(const char*)memchr(from?base+from-1:base,'\n',from?len-(from-1):len);
 if(!nl)return;
 const char* p=nl+1;
 const char* lim=base+min(to,len);
 DateMemo3* const memo=date_memo3();
 const int64_t wstart=window_start;const uint64_t wlen=(uint64_t)(window_end-window_start);
 const __m256i vnl=_mm256_set1_epi8('\n'),vc=_mm256_set1_epi8(','),vq=_mm256_set1_epi8('"');
 PendUser<PRec> pend[V3_BATCH];PendUser<PRec>* pp=pend;
 AVec<PRec>* const buckets=tl.eb.data();KeyTable::Slot* const slots=tab.slots;const size_t tmask=tab.mask;
 while(p<lim){
  const char* next=nullptr;bool done=false;
  do{
   // Window 1: 64 bytes from the line start; must hold the first three commas (fields 0-2).
   const __m256i x0=_mm256_loadu_si256((const __m256i*)p),x1=_mm256_loadu_si256((const __m256i*)(p+32));
   const uint64_t c=mask64_v3(x0,x1,vc),qa=mask64_v3(x0,x1,vq),na=mask64_v3(x0,x1,vnl);
   const uint64_t cA=_blsr_u64(c),cB=_blsr_u64(cA),cR=_blsr_u64(cB);
   const uint64_t m1=_blsmsk_u64(cB); // bytes up to and including the third comma
   const uint64_t q1=qa&m1,st1=((c^cB)<<1)|1,en1=c^cR;
   uint64_t sq1,op1;const uint64_t err1=quote_errors_v3(q1,st1,en1,sq1,op1);
   // Window 2: 64 bytes from the start of field 3; must hold the last three commas and the newline.
   const char* const b=p+_tzcnt_u64(cB)+1;
   const __m256i y0=_mm256_loadu_si256((const __m256i*)b),y1=_mm256_loadu_si256((const __m256i*)(b+32));
   const uint64_t nb=mask64_v3(y0,y1,vnl);
   const uint64_t keep=_blsmsk_u64(nb)>>1; // bytes before the newline
   const uint64_t d=mask64_v3(y0,y1,vc)&keep,qb=mask64_v3(y0,y1,vq)&keep;
   const uint64_t st2=(d<<1)|1,en2=d|_blsi_u64(nb);
   uint64_t sq2,op2;const uint64_t err2=quote_errors_v3(qb,st2,en2,sq2,op2);
   if(__builtin_expect(cB==0||nb==0||_mm_popcnt_u64(d)!=3||(err1|err2|(na&m1))!=0,0))break;
   next=b+_tzcnt_u64(nb)+1;done=true; // from here on the line is well-formed, 7 fields
   // Content of each field: starts move one up past an opening quote, ends one down onto a closing quote.
   uint64_t B1=st1+sq1,E1=en1-(op1>>1),B2=st2+sq2,E2=en2-(op2>>1);
   const unsigned z0=(unsigned)(q1&1);
   const char* const s0=p+z0;const unsigned l0=(unsigned)_tzcnt_u64(E1)-z0;
   if(l0==0)break; // no event_id: dropped
   const unsigned z3=(unsigned)(qb&1);
   const char* const s3=b+z3;const unsigned l3=(unsigned)_tzcnt_u64(E2)-z3;
   // ingest_ts is F1 in generated data; anything else is left to the general handler.
   int64_t tod3,ingestDay;
   if(__builtin_expect(l3!=20||s3[19]!='Z'||(tod3=tod_v3(s3))<0||!date3(s3,memo,ingestDay),0)){done=false;break;}
   const int64_t ingest=ingestDay+tod3;
   B1=_blsr_u64(B1);E1=_blsr_u64(E1);
   const unsigned b1=(unsigned)_tzcnt_u64(B1),l1=(unsigned)_tzcnt_u64(E1)-b1;
   B1=_blsr_u64(B1);E1=_blsr_u64(E1);
   const unsigned b2=(unsigned)_tzcnt_u64(B1),l2=(unsigned)_tzcnt_u64(E1)-b2;
   B2=_blsr_u64(B2);E2=_blsr_u64(E2);
   const unsigned b4=(unsigned)_tzcnt_u64(B2),l4=(unsigned)_tzcnt_u64(E2)-b4;
   B2=_blsr_u64(B2);E2=_blsr_u64(E2);
   // field 5 (product_id) is empty iff its start mark and end mark are the same bit: the lowest bit of
   // B2 is its start mark, and no end mark of a later field can sit that low.
   const bool has5=(_blsi_u64(B2)&E2)==0;
   const char* const s1=p+b1;const char* const s2=p+b2;
   PRec r;r.id=s0;r.idlen=l0;r.h=hkey_v3(s0,l0);r.ingest=ingest;r.ts=0;r.user=0;r.price=0;r.cat=0;r.flags=0;
   const unsigned kind=event_kind(string_view(b+b4,l4));
   bool valid=false;
   if(kind&&l1!=0&&has5){
    int64_t ts;bool tsOk;
    uint64_t a8,b8;uint16_t a2,b2_;
    // An F1 ts on the same calendar day as ingest_ts (the usual case) needs no date work at all.
    if(l2==20&&s2[19]=='Z'&&(memcpy(&a8,s2,8),memcpy(&b8,s3,8),memcpy(&a2,s2+8,2),memcpy(&b2_,s3+8,2),((a8^b8)|(uint64_t)(a2^b2_))==0)){
     const int64_t tod2=tod_v3(s2);
     if(tod2>=0){ts=ingestDay+tod2;tsOk=true;}else tsOk=parse_ts(string_view(s2,l2),ts);
    }else tsOk=ts_v3(s2,l2,memo,ts);
    // window_start <= ts < window_end as one unsigned compare (the window is far shorter than 2^63 ms)
    if(tsOk&&(uint64_t)ts-(uint64_t)wstart<wlen&&ingest-ts<=DAY){
     int32_t price=0;const bool purchase=kind==3;
     if(purchase){
      const unsigned b5=(unsigned)_tzcnt_u64(B2),l5=(unsigned)_tzcnt_u64(E2)-b5;
      B2=_blsr_u64(B2);E2=_blsr_u64(E2);
      const unsigned b6=(unsigned)_tzcnt_u64(B2),l6=(unsigned)_tzcnt_u64(E2)-b6;
      if(price_value(string_view(b+b6,l6),price)){
       const uint32_t ci=products.find(string_view(b+b5,l5));
       r.cat=ci==KeyTable::NONE?0:(uint16_t)ci;r.flags=F_VALID|F_PURCHASE;r.ts=ts;r.price=price;valid=true;
      }
     }else{r.flags=F_VALID;r.ts=ts;valid=true;}
    }
   }
   AVec<PRec>& vec=buckets[(size_t)(((r.h>>40)*PE)>>24)];
   if(__builtin_expect(vec.size()==vec.capacity(),0)){resolve_users(pend,pp,tab,tl);pp=pend;vec.reserve(max<size_t>(16,2*vec.capacity()));} // grow now: nothing waits on the old storage, and the record pushed below stays put
   if(valid){
    uint64_t k0;uint32_t k1;
    if(__builtin_expect(!KeyTable::pack(string_view(s1,l1),k0,k1),0)){done=false;break;} // long user ID: nothing has been stored yet
    const size_t j=KeyTable::hash_packed(k0,k1)&tmask;
    __builtin_prefetch(&slots[j],0,1);
    PendUser<PRec>& pu=*pp++;pu.k0=k0;pu.j=(uint32_t)j;pu.u=s1;pu.rec=vec.data()+vec.size();pu.k1=k1;
   }
   vec.push_back(r);
   if(pp==pend+V3_BATCH){resolve_users(pend,pp,tab,tl);pp=pend;}
  }while(0);
  if(!done){
   // Keep the order of lines: finish the waiting ones before the general handler stores anything.
   resolve_users(pend,pp,tab,tl);pp=pend;
   const char* comma[7];int ncomma,nquote;const char* lineEnd;string_view f[7];
   next=scan_line(p,end,comma,7,ncomma,nquote,lineEnd);
   if(take_fields(p,lineEnd,comma,ncomma,nquote,7,f))general(f);
  }
  p=next;
 }
 resolve_users(pend,pp,tab,tl);
}
// The same for click lines (4 fields, one 64-byte window).
template<class GEN> SS_V3 static void clicks_v3(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& camps,size_t PC,int64_t clickStart,GEN&& general){
 if(from>=len)return;
 const char* end=base+len;
 const char* nl=(const char*)memchr(from?base+from-1:base,'\n',from?len-(from-1):len);
 if(!nl)return;
 const char* p=nl+1;
 const char* lim=base+min(to,len);
 DateMemo3* const memo=date_memo3();
 const uint64_t wlen=(uint64_t)(window_end-clickStart);
 const __m256i vnl=_mm256_set1_epi8('\n'),vc=_mm256_set1_epi8(','),vq=_mm256_set1_epi8('"');
 PendUser<CRec> pend[V3_BATCH];PendUser<CRec>* pp=pend;
 AVec<CRec>* const buckets=tl.cb.data();KeyTable::Slot* const slots=tab.slots;const size_t tmask=tab.mask;
 while(p<lim){
  const char* next=nullptr;bool done=false;
  do{
   const __m256i x0=_mm256_loadu_si256((const __m256i*)p),x1=_mm256_loadu_si256((const __m256i*)(p+32));
   const uint64_t nb=mask64_v3(x0,x1,vnl);
   const uint64_t keep=_blsmsk_u64(nb)>>1;
   const uint64_t c=mask64_v3(x0,x1,vc)&keep,q=mask64_v3(x0,x1,vq)&keep;
   const uint64_t st=(c<<1)|1,en=c|_blsi_u64(nb);
   uint64_t sq,op;const uint64_t err=quote_errors_v3(q,st,en,sq,op);
   if(__builtin_expect(nb==0||_mm_popcnt_u64(c)!=3||err!=0,0))break;
   next=p+_tzcnt_u64(nb)+1;done=true; // well-formed, 4 fields
   uint64_t B=st+sq,E=en-(op>>1);
   const unsigned z0=(unsigned)(q&1);
   const char* const s0=p+z0;const unsigned l0=(unsigned)_tzcnt_u64(E)-z0;
   B=_blsr_u64(B);E=_blsr_u64(E);
   const unsigned b1=(unsigned)_tzcnt_u64(B),l1=(unsigned)_tzcnt_u64(E)-b1;
   if(l0==0||l1==0)break;
   B=_blsr_u64(B);E=_blsr_u64(E);
   const unsigned b2=(unsigned)_tzcnt_u64(B),l2=(unsigned)_tzcnt_u64(E)-b2;
   B=_blsr_u64(B);E=_blsr_u64(E);
   const unsigned b3=(unsigned)_tzcnt_u64(B),l3=(unsigned)_tzcnt_u64(E)-b3;
   // Copies of a click are identical, so validating before the dedup keeps the same clicks.
   int64_t ts=0;
   if(!ts_v3(p+b3,l3,memo,ts)||(uint64_t)ts-(uint64_t)clickStart>=wlen)break; // clickStart <= ts < window_end
   uint64_t k0;uint32_t k1;
   const string_view camp(p+b2,l2);
   const uint32_t ci=__builtin_expect(KeyTable::pack(camp,k0,k1),1)?camps.find_packed(k0,k1):camps.find(camp);if(ci==KeyTable::NONE)break;
   const char* const s1=p+b1;
   if(__builtin_expect(!KeyTable::pack(string_view(s1,l1),k0,k1),0)){done=false;break;} // long user ID
   const size_t j=KeyTable::hash_packed(k0,k1)&tmask;
   __builtin_prefetch(&slots[j],0,1);
   CRec r;r.id=s0;r.idlen=l0;r.h=hkey_v3(s0,l0);r.ts=ts;r.user=0;r.campaign=(uint16_t)ci;r.flags=0;
   AVec<CRec>& vec=buckets[(size_t)(((r.h>>40)*PC)>>24)];
   if(__builtin_expect(vec.size()==vec.capacity(),0)){resolve_users(pend,pp,tab,tl);pp=pend;vec.reserve(max<size_t>(16,2*vec.capacity()));} // grow now: nothing waits on the old storage, and the record pushed below stays put
   PendUser<CRec>& pu=*pp++;pu.k0=k0;pu.j=(uint32_t)j;pu.u=s1;pu.rec=vec.data()+vec.size();pu.k1=k1;
   vec.push_back(r);
   if(pp==pend+V3_BATCH){resolve_users(pend,pp,tab,tl);pp=pend;}
  }while(0);
  if(!done){
   resolve_users(pend,pp,tab,tl);pp=pend;
   const char* comma[4];int ncomma,nquote;const char* lineEnd;string_view f[4];
   next=scan_line(p,end,comma,4,ncomma,nquote,lineEnd);
   if(take_fields(p,lineEnd,comma,ncomma,nquote,4,f))general(f);
  }
  p=next;
 }
 resolve_users(pend,pp,tab,tl);
}
static void run(const fs::path& data,const fs::path& scratch,const fs::path& output){
 auto __time0=chrono::steady_clock::now();
 auto __mark=[&](const char *label){auto now=chrono::steady_clock::now();cerr<<"TIME "<<label<<" "<<chrono::duration<double>(now-__time0).count()<<"\n";__time0=now;};
 optHuge=!getenv("SS_NOHUGE");optPrefault=!getenv("SS_NOPREFAULT");optAsyncFree=!getenv("SS_NOASYNCFREE");
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
 vector<Region> eventMaps;const char* sampleBase=nullptr;size_t sampleLen=0;
 auto add_files=[&](const fs::path& dir,bool click,uint64_t& bytes){
  for(const auto& p:csv_files(dir)){Mapped m=map_file(p);if(!m.len)continue;bytes+=m.len;
   if(!click){eventMaps.push_back({(void*)m.base,((m.len+4095)&~size_t(4095))+4096});
    if(m.len>sampleLen){sampleLen=m.len;sampleBase=m.base;}}
   for(size_t from=0;from<m.len;from+=CH)chunks.push_back({m.base,m.len,from,from+CH,click});}
 };
 add_files(data/"events",false,eventBytes);
 add_files(data/"clicks",true,clickBytes);
 // Lines per byte, measured on the start of the largest event file, sizes the bucket buffers tightly.
 uint64_t estEvents=eventBytes/70;
 if(sampleLen>4096){const size_t n=min<size_t>(sampleLen,size_t(4)<<20);const uint64_t nl=(uint64_t)count_newlines(sampleBase,n);
  if(nl>16)estEvents=(uint64_t)((double)eventBytes*(double)nl/(double)n)+1;}
 const uint64_t estClicks=clickBytes/36;
 __mark("MAP");
 // ---- Linked IDs: parse link files in parallel, intern both IDs lock-free, union with compare-and-swap. ----
 vector<Chunk> linkChunks;uint64_t linkLines=0;
 for(const auto& p:csv_files(data/"links")){Mapped m=map_file(p);if(!m.len)continue;
  ++linkLines; // a last line without '\n'
  for(size_t from=0;from<m.len;from+=CH)linkChunks.push_back({m.base,m.len,from,from+CH,false});}
 #pragma omp parallel for num_threads(T) schedule(dynamic,1) reduction(+:linkLines)
 for(size_t c=0;c<linkChunks.size();++c){const Chunk& ck=linkChunks[c];linkLines+=count_newlines(ck.base+ck.from,min(ck.to,ck.len)-ck.from);}
 const uint64_t maxLinked=2*linkLines; // every link line names at most two IDs
 // Room for IDs that only appear in events and clicks: about 3x what the generator produces.
 // If a dataset has more, the extra ones take the thread-private fallback path (slower, still exact).
 uint64_t maxNew64=(estEvents+estClicks)/28+4096;
 if(const char* v=getenv("SS_TEST_MAXNEW"))maxNew64=strtoull(v,nullptr,10); // tests force the fallback path with a tiny value
 if(maxLinked+maxNew64>=0x60000000u)throw runtime_error("Too many IDs");
 const uint32_t maxIds=(uint32_t)(maxLinked+maxNew64);
 KeyTable tab((size_t)maxIds*8/5); // at most 62% full even if every allowed ID shows up
 tab.begin_parallel(0,maxIds);
 // Union-find over linked IDs. Both arrays hold "value XOR index", so an untouched (all-zero) entry
 // means "itself": they come from mmap as zero pages, need no initialising pass, and the part beyond
 // the IDs actually seen is never touched. parent(x) <= x always (a root links to a smaller root), so
 // the forest can never form a cycle. best(r): the ID with the byte-wise smallest name seen so far in root r's group.
 struct ZeroPages{
  void* p;size_t n;
  explicit ZeroPages(size_t bytes):n(bytes){p=mmap(nullptr,n,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);if(p==MAP_FAILED)throw bad_alloc();}
  void release(){if(p){munmap(p,n);p=nullptr;}}
  ~ZeroPages(){release();}
  ZeroPages(const ZeroPages&)=delete;ZeroPages& operator=(const ZeroPages&)=delete;
 };
 static_assert(sizeof(atomic<uint32_t>)==4);
 ZeroPages parentMem((maxLinked+1)*sizeof(uint32_t)),bestMem((maxLinked+1)*sizeof(uint32_t));
 atomic<uint32_t>* const parentX=(atomic<uint32_t>*)parentMem.p;atomic<uint32_t>* const bestX=(atomic<uint32_t>*)bestMem.p;
 auto find_root=[&](uint32_t u)->uint32_t{
  for(;;){
   uint32_t su=parentX[u].load(memory_order_relaxed);if(!su)return u;
   const uint32_t p=su^u;
   const uint32_t sp=parentX[p].load(memory_order_relaxed);if(!sp)return p;
   const uint32_t gp=sp^p;
   parentX[u].compare_exchange_weak(su,gp^u); // path halving; losing the race is harmless
   u=gp;
  }
 };
 atomic<size_t> nextLink{0};
 #pragma omp parallel num_threads(T)
 {
  // Lines are handled in small batches so the table slots of a batch, and then the parent entries of
  // its IDs, are on their way into the cache before they are needed.
  static constexpr int LB=16;
  struct LinkPend{string_view f[2];uint64_t k0[2];uint32_t k1[2];uint32_t j[2];}; // k1 == 0: key too long to pack
  LinkPend pend[LB];uint32_t ida[LB],idb[LB];
  const size_t tmask=tab.mask;
  auto resolve=[&](int nb){
   for(int i=0;i<nb;++i){const LinkPend& q=pend[i];
    // cannot be full: the table holds 2 IDs per link line
    const uint32_t a=q.k1[0]?tab.find_or_add_packed(q.k0[0],q.k1[0],q.j[0],q.f[0]):tab.find_or_add_long(q.f[0]);
    const uint32_t b=q.k1[1]?tab.find_or_add_packed(q.k0[1],q.k1[1],q.j[1],q.f[1]):tab.find_or_add_long(q.f[1]);
    ida[i]=a;idb[i]=b;__builtin_prefetch(&parentX[a],1,1);__builtin_prefetch(&parentX[b],1,1);
   }
   for(int i=0;i<nb;++i){
    uint32_t a=ida[i],b=idb[i];
    for(;;){
     a=find_root(a);b=find_root(b);if(a==b)break;
     if(a<b)swap(a,b);
     uint32_t expect=0;if(parentX[a].compare_exchange_strong(expect,b^a))break; // a is still a root: link it under b
    }
   }
  };
  for(;;){
   size_t c=nextLink.fetch_add(1,memory_order_relaxed);if(c>=linkChunks.size())break;
   const Chunk& ck=linkChunks[c];
   // Every data line that begins in [from,to), as in for_lines; exactly two non-empty fields or the line is skipped.
   if(ck.from>=ck.len)continue;
   const char* const end=ck.base+ck.len;
   const char* nl=(const char*)memchr(ck.from?ck.base+ck.from-1:ck.base,'\n',ck.from?ck.len-(ck.from-1):ck.len);
   if(!nl)continue;
   const char* const lim=ck.base+min(ck.to,ck.len);
   int nb=0;
   const __m128i vnl=_mm_set1_epi8('\n'),vc=_mm_set1_epi8(','),vq=_mm_set1_epi8('"');
   for(const char* p=nl+1;p<lim;){
    LinkPend& q=pend[nb];const char* next;bool ok,done=false;
    // Usual shape: a line of under 32 bytes with one comma and no quote, cut from two 16-byte compares.
    if(end-p>=32){
     const __m128i x0=_mm_loadu_si128((const __m128i*)p),x1=_mm_loadu_si128((const __m128i*)(p+16));
     const uint32_t mnl=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vnl))|((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vnl))<<16);
     if(mnl){
      const unsigned L=(unsigned)__builtin_ctz(mnl);const uint32_t keep=(1u<<L)-1;
      const uint32_t mc=((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vc))|((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vc))<<16))&keep;
      const uint32_t mq=((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vq))|((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vq))<<16))&keep;
      if(!mq&&mc&&!(mc&(mc-1))){
       const unsigned P=(unsigned)__builtin_ctz(mc);
       done=true;next=p+L+1;ok=P>0&&P+1<L; // both IDs non-empty
       q.f[0]=string_view(p,P);q.f[1]=string_view(p+P+1,L-P-1);
      }
     }
    }
    if(!done){ // anything else: the general splitter decides
     const char* comma[2];int ncomma,nquote;const char* lineEnd;
     next=scan_line(p,end,comma,2,ncomma,nquote,lineEnd);
     ok=take_fields(p,lineEnd,comma,ncomma,nquote,2,q.f)&&!q.f[0].empty()&&!q.f[1].empty();
    }
    if(ok){
     for(int k=0;k<2;++k){
      if(KeyTable::pack(q.f[k],q.k0[k],q.k1[k])){q.j[k]=(uint32_t)(KeyTable::hash_packed(q.k0[k],q.k1[k])&tmask);__builtin_prefetch(&tab.slots[q.j[k]],1,1);}
      else q.k1[k]=0;
     }
     if(++nb==LB){resolve(nb);nb=0;}
    }
    p=next;
   }
   resolve(nb);
  }
 }
 const uint32_t nlinked=tab.nextId.load();
 if(getenv("SS_TEST_MAXNEW"))tab.idLimit=min<uint64_t>(tab.idLimit,(uint64_t)nlinked+maxNew64); // tests: leave room for only that many unlinked IDs
 __mark("LINKREAD");
 // Flatten: every linked ID that is not its group's root offers its name to the root (the byte-wise
 // smallest wins) and has its table entry pointed at the root, which is the person number.
 {
  PName* names=tab.newNames;
  #pragma omp parallel num_threads(T)
  {
   #pragma omp for schedule(static)
   for(uint32_t i=0;i<nlinked;++i){
    const uint32_t r=find_root(i);if(r==i)continue;
    uint32_t sc=bestX[r].load(memory_order_relaxed);
    while(name_less(names[i],names[sc^r])&&!bestX[r].compare_exchange_weak(sc,i^r)){}
    uint64_t k0;uint32_t k1;
    if(KeyTable::pack(names[i].view(),k0,k1))tab.set_packed(k0,k1,r); // longer keys are in tab.longs, below
   }
   // A root's own name is no longer needed once every member has been compared, and no other person reads it.
   #pragma omp for schedule(static)
   for(uint32_t i=0;i<nlinked;++i)if(!parentX[i].load(memory_order_relaxed)){const uint32_t sb=bestX[i].load(memory_order_relaxed);if(sb)names[i]=names[sb^i];}
  }
  for(auto& l:tab.longs)if(l.v<nlinked)l.v=find_root(l.v);
 }
 parentMem.release();bestMem.release();
 __mark("FLATTEN");
 __mark("LINKS"); cerr<<"linked IDs: "<<nlinked<<"\n";
 FastMap<uint16_t> categories;vector<string> catnames;
 cat_id("unknown",categories,catnames);
 KeyTable products(1<<17); // product_id -> category number
 // Categories repeat: a small memo of the last category texts seen saves most of the string-map lookups.
 struct CatMemo{const char* p;uint32_t n;uint16_t id;};
 CatMemo catMemo[64]={};
 read_pairs(data/"products.csv",[&](const string_view* f,bool ok){
  if(!ok)throw runtime_error("Malformed clean products.csv");
  const string_view c=f[1];
  uint64_t w;memcpy(&w,c.data(),8);w&=LOW_BYTES[min<size_t>(c.size(),8)]; // mapped input: reading past the end is safe
  CatMemo& e=catMemo[((w^c.size())*0x9e3779b97f4a7c15ULL)>>58];
  uint16_t id;
  if(e.p&&e.n==c.size()&&memcmp(e.p,c.data(),c.size())==0)id=e.id;
  else{id=cat_id(c,categories,catnames);e.p=c.data();e.n=(uint32_t)c.size();e.id=id;}
  products.intern(f[0],id);
 });
 vector<Camp> campaigns;KeyTable camps(1<<13); // campaign_id -> campaign number
 read_pairs(data/"campaigns.csv",[&](const string_view* f,bool ok){
  if(!ok)throw runtime_error("Malformed clean campaigns.csv");
  if(campaigns.size()>=65534)throw runtime_error("Too many campaigns");
  const uint32_t n=(uint32_t)campaigns.size();if(camps.intern(f[0],n)==n)campaigns.push_back({string(f[0]),string(f[1])});});
 uint16_t organic=(uint16_t)campaigns.size();campaigns.push_back({"organic","organic"});

 // ---- Parse every event and click line in parallel, bucketing by ID hash. ----
 const size_t PE=pow2_parts(eventBytes/70),PC=pow2_parts(clickBytes/40);
 const size_t UP=pow2_parts(eventBytes/70); // person buckets for the last stage
 // SS_ISA=sse2 forces the portable splitter (used by tests to check both paths give the same tables).
 bool useV3=__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("popcnt")&&__builtin_cpu_supports("lzcnt");
 if(const char* isa=getenv("SS_ISA"))if(string_view(isa)=="sse2")useV3=false;
 cerr<<"splitter: "<<(useV3?"avx2":"sse2")<<"\n";
 vector<ThreadState> tls(T);
 atomic<size_t> nextChunk{0};
 const int64_t clickStart=window_start-WEEK;
 #pragma omp parallel num_threads(T)
 {
  ThreadState& tl=tls[omp_get_thread_num()];
  tl.eb.resize(PE);tl.cb.resize(PC);tl.uev.resize(UP);tl.ucl.resize(UP);
  {const size_t re=(size_t)(estEvents/((uint64_t)T*PE)*9/8)+16,rc=(size_t)(estClicks/((uint64_t)T*PC)*9/8)+16;
   const size_t need=PE*(re*sizeof(PRec)+64)+PC*(rc*sizeof(CRec)+64);
   tlArena.stage=0;tlArena.add_region(need+(size_t(4)<<20),need); // one pre-faulted block per thread for its buckets
   for(auto& v:tl.eb)v.reserve(re);
   for(auto& v:tl.cb)v.reserve(rc);}
  auto user_of=[&](string_view u)->uint32_t{
   uint32_t g=tab.find_or_add(u);
   if(g!=KeyTable::NONE)return g;
   return LOCAL_USER|tl.users.intern(u);
  };
  auto warm_user=[&](const string_view* f){tab.prefetch(f[1]);}; // user_id is field 1 in both files
  auto on_event=[&](const string_view* f){
   if(f[0].empty())return;
   // ingest_ts is always F1 in generated data; anything else goes through the general parser.
   int64_t ingest=0,ingestDay=0,ms;const char* pi=f[3].data();
   const bool ingestF1=f[3].size()==20&&pi[19]=='Z'&&hms_ms(pi,ms)&&f1_date(pi,ingestDay);
   if(ingestF1)ingest=ingestDay+ms;else if(!parse_ts(f[3],ingest))return;
   PRec r;r.id=f[0].data();r.idlen=(uint32_t)f[0].size();r.h=hkey(f[0]);r.ingest=ingest;r.ts=0;r.user=0;r.price=0;r.cat=0;r.flags=0;
   const unsigned kind=event_kind(f[4]);
   if(kind&&!f[1].empty()&&!f[5].empty()){
    int64_t ts;bool tsOk;const char* pt=f[2].data();
    uint64_t a8,b8;uint16_t a2,b2;
    // An F1 ts on the same calendar day as ingest_ts (the usual case) needs no date work at all.
    if(ingestF1&&f[2].size()==20&&pt[19]=='Z'&&(memcpy(&a8,pt,8),memcpy(&b8,pi,8),memcpy(&a2,pt+8,2),memcpy(&b2,pi+8,2),a8==b8&&a2==b2)&&hms_ms(pt,ms)){ts=ingestDay+ms;tsOk=true;}
    else tsOk=parse_ts_fast(f[2],ts);
    if(tsOk&&ts>=window_start&&ts<window_end&&ingest-ts<=DAY){
     int32_t price=0;const bool purchase=kind==3;
     if(!purchase||price_value(f[6],price)){
      r.flags=F_VALID|(purchase?F_PURCHASE:0);r.ts=ts;r.price=price;r.user=user_of(f[1]);
      if(purchase){const uint32_t c=products.find(f[5]);r.cat=c==KeyTable::NONE?0:(uint16_t)c;}
     }
    }
   }
   tl.eb[(size_t)(((r.h>>40)*PE)>>24)].push_back(r);
  };
  auto on_click=[&](const string_view* f){
   if(f[0].empty())return;
   // Copies of a click are identical, so validating before the dedup keeps the same clicks.
   int64_t ts=0;
   if(f[1].empty()||!parse_ts_fast(f[3],ts)||ts<clickStart||ts>=window_end)return;
   const uint32_t ci=camps.find(f[2]);if(ci==KeyTable::NONE)return;
   CRec r;r.id=f[0].data();r.idlen=(uint32_t)f[0].size();r.h=hkey(f[0]);r.ts=ts;r.user=user_of(f[1]);r.campaign=(uint16_t)ci;r.flags=0;
   tl.cb[(size_t)(((r.h>>40)*PC)>>24)].push_back(r);
  };
  for(;;){
   size_t c=nextChunk.fetch_add(1,memory_order_relaxed);if(c>=chunks.size())break;
   const Chunk& ck=chunks[c];
   if(useV3){
    if(ck.click)clicks_v3(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click);
    else events_v3(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event);
   }else{
    if(ck.click)for_records<4>(ck.base,ck.len,ck.from,ck.to,warm_user,on_click);
    else for_records<7>(ck.base,ck.len,ck.from,ck.to,warm_user,on_event);
   }
  }
 }
 const uint32_t totalIds=tab.end_parallel();
 __mark("PARSE");
 // ---- Fallback only: IDs that did not fit in the shared table get their person number here. ----
 // From here on a person is a compact number: shared-table IDs keep theirs (< totalIds), fallback IDs follow.
 vector<vector<uint32_t>> trans(T);vector<string_view> extraNames;
 const uint32_t extraBase=maxIds;
 for(int t=0;t<T;++t){auto& keys=tls[t].users.keys;trans[t].resize(keys.size());
  for(size_t i=0;i<keys.size();++i){
   if(extraNames.size()>=0x08000000u)throw runtime_error("Too many IDs");
   uint32_t n=extraBase+(uint32_t)extraNames.size();
   uint32_t id=tab.intern(keys[i],n);
   if(id==n)extraNames.push_back(keys[i]);
   trans[t][i]=id<extraBase?id:totalIds+(id-extraBase);
  }
 }
 const PName* personNames=tab.newNames;
 auto name_of=[&](uint32_t person)->string_view{
  if(person<totalIds)return personNames[person].view();
  return extraNames[person-totalIds];
 };
 const uint32_t newCount=totalIds-nlinked;
 cerr<<"unlinked IDs: "<<newCount<<" (+"<<extraNames.size()<<" via fallback)\n";
 // ---- Person buckets: bucket = person >> pshift, so a bucket is a dense range of W person numbers
 // and the last stage can group by person with one counting pass. Aim for about 16k events per bucket.
 const uint64_t npersons=(uint64_t)totalIds+extraNames.size();
 uint64_t parsedEvents=0;
 for(int t=0;t<T;++t)for(const auto& v:tls[t].eb)parsedEvents+=v.size();
 unsigned pshift=0;
 {const uint64_t want=max<uint64_t>(1,parsedEvents/16384);while(pshift<16&&(npersons>>pshift)>want)++pshift;}
 if(const char* v=getenv("SS_TEST_PSHIFT")){int s=atoi(v);if(s>=0&&s<=16)pshift=(unsigned)s;} // tests force odd bucket widths
 const uint32_t W=1u<<pshift;
 const size_t UPn=(size_t)((npersons?npersons-1:0)>>pshift)+1;
 // Survivors by person bucket, one list per thread and bucket (tls[t].uev/ucl are not used any more).
 struct Survivors{vector<EvList> ev;vector<ClickList> cl;SlabPool pool;};
 vector<Survivors> surv(T);
 for(int t=0;t<T;++t){surv[t].ev.resize(UPn);surv[t].cl.resize(UPn);tls[t].uev.clear();tls[t].ucl.clear();}
 // ---- Dedup each ID bucket on its own; survivors go to person buckets. ----
 uint64_t dedupCount=0,surviving=0,survivingClicks=0;
 #pragma omp parallel num_threads(T) reduction(+:dedupCount,surviving,survivingClicks)
 {
  const int tid=omp_get_thread_num();
  EvList* const myEv=surv[tid].ev.data();ClickList* const myCl=surv[tid].cl.data();SlabPool& pool=surv[tid].pool;
  // Slot = tag<<ib | (record number+1), 0 = empty; ib bits are enough for the bucket's record count.
  // The table has two slots per record; the low half of the hash picks the slot, the high half is the tag.
  Scratch<uint32_t> slotBuf;Scratch<Ev*> emitBuf;vector<size_t> off(T+1);
  auto bucket_size=[&](const auto& buckets,size_t p)->size_t{return p<buckets.size()?buckets[p].size():0;};
  // One pass per bucket: a record whose ID is new is passed on at once (if valid). When a later copy
  // turns out to be the earlier ingested one, the event already passed on is turned into a tombstone:
  // its person becomes the first number past its bucket's range, which the last stage never visits.
  #pragma omp for schedule(dynamic,1) nowait
  for(size_t p=0;p<PE;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=bucket_size(tls[t].eb,p);}off[T]=n;
   if(!n)continue;
   if(n>=0x7fffffffu){fprintf(stderr,"ERROR event ID bucket too large\n");_exit(1);}
   const size_t cap=2*n+2;
   uint32_t* const S=slotBuf.get(cap);memset(S,0,cap*sizeof(uint32_t));
   Ev** const emitted=emitBuf.get(n); // where record g's event went (only read for valid winners)
   const unsigned ib=(unsigned)bit_width(n);const uint32_t imask=(1u<<ib)-1;
   size_t dups=0,dead=0;
   const int64_t ws=window_start;
   for(int t=0;t<T;++t){
    const size_t m=bucket_size(tls[t].eb,p);if(!m)continue;
    const PRec* r=tls[t].eb[p].data();const PRec* const rend=r+m;uint32_t g1=(uint32_t)off[t]+1;const uint32_t* const tr=trans[t].data();
    for(;r!=rend;++r,++g1){
     const uint64_t h=r->h;const uint32_t tg=(uint32_t)(h>>32)<<ib;
     size_t j=(size_t)(((h&0xffffffffu)*cap)>>32);
     for(uint32_t e;(e=S[j])!=0;){
      if((e^tg)<=imask){ // same tag: compare the IDs themselves
       const uint32_t og=(e&imask)-1;int ot=0;while(og>=off[ot+1])++ot;
       const PRec& o=tls[ot].eb[p][og-off[ot]];
       if(o.idlen==r->idlen&&memcmp(o.id,r->id,r->idlen)==0){
        // Keep the earliest ingested copy; copies with equal ingest_ts are identical.
        ++dups;
        if(r->ingest>=o.ingest)goto next_event;
        if(o.flags&F_VALID){Ev* const gone=emitted[og];gone->person=((gone->person>>pshift)+1)<<pshift;++dead;}
        break;
       }
      }
      if(++j==cap)j=0;
     }
     S[j]=tg|g1;
     {
      const unsigned fl=r->flags;
      if(fl&F_VALID){
       uint32_t u=r->user;if(u&LOCAL_USER)u=tr[u&~LOCAL_USER];
       Ev* const out=myEv[u>>pshift].slot(pool);
       emitted[g1-1]=out;
       out->k=((uint64_t)(r->ts-ws)<<EV_TS)|((uint64_t)(fl&F_PURCHASE)<<23)|(uint64_t)(uint32_t)r->price;
       const uint64_t w=(uint64_t)u|((uint64_t)r->cat<<32);memcpy((void*)&out->person,&w,8);
      }
     }
     next_event:;
    }
   }
   for(int t=0;t<T;++t)if(p<tls[t].eb.size())tls[t].eb[p]=AVec<PRec>();
   surviving-=dead;dedupCount+=n-dups;
  }
  // Copies of a click are identical, so the first one seen is kept.
  #pragma omp for schedule(dynamic,1)
  for(size_t p=0;p<PC;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=bucket_size(tls[t].cb,p);}off[T]=n;
   if(!n)continue;
   if(n>=0x7fffffffu){fprintf(stderr,"ERROR click ID bucket too large\n");_exit(1);}
   const size_t cap=2*n+2;
   uint32_t* const S=slotBuf.get(cap);memset(S,0,cap*sizeof(uint32_t));
   const unsigned ib=(unsigned)bit_width(n);const uint32_t imask=(1u<<ib)-1;
   for(int t=0;t<T;++t){
    const size_t m=bucket_size(tls[t].cb,p);if(!m)continue;
    const CRec* r=tls[t].cb[p].data();const CRec* const rend=r+m;uint32_t g1=(uint32_t)off[t]+1;const uint32_t* const tr=trans[t].data();
    for(;r!=rend;++r,++g1){
     const uint64_t h=r->h;const uint32_t tg=(uint32_t)(h>>32)<<ib;
     size_t j=(size_t)(((h&0xffffffffu)*cap)>>32);
     for(uint32_t e;(e=S[j])!=0;){
      if((e^tg)<=imask){
       const uint32_t og=(e&imask)-1;int ot=0;while(og>=off[ot+1])++ot;
       const CRec& o=tls[ot].cb[p][og-off[ot]];
       if(o.idlen==r->idlen&&memcmp(o.id,r->id,r->idlen)==0)goto next_click;
      }
      if(++j==cap)j=0;
     }
     S[j]=tg|g1;
     {
      uint32_t u=r->user;if(u&LOCAL_USER)u=tr[u&~LOCAL_USER];
      Click* const out=myCl[u>>pshift].slot(pool);
      out->key=((uint64_t)(r->ts-clickStart)<<16)|r->campaign;out->id=r->id;out->person=u;out->idlen=r->idlen;
     }
     next_click:;
    }
   }
   for(int t=0;t<T;++t)if(p<tls[t].cb.size())tls[t].cb[p]=AVec<CRec>();
  }
 }
 for(int t=0;t<T;++t){for(const auto& L:surv[t].ev)surviving+=L.size();for(const auto& L:surv[t].cl)survivingClicks+=L.size();}
 cerr<<"deduplicated events: "<<dedupCount<<", surviving: "<<surviving<<"\n";
 cerr<<"surviving clicks: "<<survivingClicks<<"\n";
 // The parsed-line buckets are dead now. Hand their memory (and the event files, when nothing points
 // into them any more) back on a side thread so process exit has less to tear down.
 {vector<Region> dead;{lock_guard<mutex> g(regionMu);dead.swap(stageRegions[0]);}
  if(!anyLongName.load()&&extraNames.empty())dead.insert(dead.end(),eventMaps.begin(),eventMaps.end());
  if(optAsyncFree)thread(unmap_regions,std::move(dead)).detach();}
 __mark("DEDUP");
 // ---- Per person bucket: order, sessionize, attribute. Each thread writes its own Parquet file. ----
 fs::create_directories(output/"user_daily");fs::create_directories(output/"campaign_daily");
 const bool debugCsv=getenv("SS_DEBUG_CSV")!=nullptr; // unit tests read these hex rows from SCRATCH_DIR
 const size_t ncamp=campaigns.size();
 const int64_t firstday=divfloor(window_start,DAY);
 // UTC days the window touches (same as nday when the window starts at midnight).
 const int ndays=(int)(divfloor(window_end-1,DAY)-firstday+1);
 const uint64_t dayOff=(uint64_t)(window_start-firstday*DAY);
 // Events are only ordered down to "cells" of 2^20 ms of event time. A cell is shorter than the session
 // gap, so two events in one cell always share a session, and whether a session ends between two
 // neighbouring cells depends only on the latest time in the first and the earliest time in the second.
 static constexpr unsigned CELL=20,CSH=EV_TS+CELL;
 const uint64_t ncell=((uint64_t)(window_end-window_start-1)>>CELL)+1;
 const unsigned cellBits=ncell>1?(unsigned)bit_width(ncell-1):0;
 unsigned digitBits=13; // cells are sorted by counting passes of at most this many bits (one pass up to ~97 days)
 if(const char* v=getenv("SS_TEST_TIMEBITS")){int s=atoi(v);if(s>=1&&s<=16)digitBits=(unsigned)s;} // tests force several passes
 struct Pass{unsigned sh;uint32_t mask;};
 vector<Pass> tpass;
 for(unsigned done=0;done<cellBits||tpass.empty();){const unsigned b=min(digitBits,cellBits-done);tpass.push_back({CSH+done,(uint32_t)((1u<<b)-1)});done+=b;if(!b)break;}
 uint32_t maxDigit=0;for(const Pass& q:tpass)maxDigit=max(maxDigit,q.mask);
 // Clicks get the same treatment (their time runs from seven days before the window), then an exact sort
 // inside each person's cells, which hold only a few clicks.
 static constexpr unsigned CCSH=16+CELL;
 const uint64_t nccell=((uint64_t)(window_end-clickStart-1)>>CELL)+1;
 const unsigned ccellBits=nccell>1?(unsigned)bit_width(nccell-1):0;
 vector<Pass> cpass;
 for(unsigned done=0;done<ccellBits||cpass.empty();){const unsigned b=min(digitBits,ccellBits-done);cpass.push_back({CCSH+done,(uint32_t)((1u<<b)-1)});done+=b;if(!b)break;}
 for(const Pass& q:cpass)maxDigit=max(maxDigit,q.mask);
 vector<vector<Totals>> threadTotals(T);vector<vector<uint64_t>> threadAssists(T);vector<string> dbg(debugCsv?T:0);
 // sessions and events of one row can never exceed the number of parsed event lines, so they are written
 // as 32-bit integers whenever that number fits (always, in practice); revenue stays 64-bit.
 const bool narrow=parsedEvents<0x7fffffffu&&!getenv("SS_TEST_WIDECOUNTS"); // tests force the 64-bit columns
 const int countType=narrow?PQ_INT32:PQ_INT64;
 const vector<PqCol> userCols={{"user_id",PQ_BYTES,false,CONV_UTF8},{"day",PQ_INT32,false,CONV_DATE},{"sessions",countType,false,CONV_NONE},
  {"events",countType,false,CONV_NONE},{"revenue_cents",PQ_INT64,false,CONV_NONE},{"top_category",PQ_BYTES,true,CONV_UTF8}};
 uint64_t num_sessions=0;
 #pragma omp parallel num_threads(T) reduction(+:num_sessions)
 {
  const int tid=omp_get_thread_num();
  // Per-thread campaign totals, laid out [day][campaign].
  auto& tot=threadTotals[tid];tot.assign(ncamp*(size_t)ndays,Totals{});
  auto& assists=threadAssists[tid];assists.assign(ncamp*(size_t)ndays,0);
  vector<uint32_t> freq(ncamp),positions(ncamp);vector<uint16_t> active(ncamp);size_t nact=0;
  vector<uint64_t> catsum(catnames.size());vector<uint16_t> touched;
  PqFile file;
  auto open_file=[&]{char name[32];snprintf(name,sizeof name,"part-%05d.parquet",tid);file.open(output/"user_daily"/name,userCols);};
  // Row buffers. A person adds at most ndays rows and the buffers are flushed between persons,
  // so the fixed-width columns never need a capacity check.
  static constexpr size_t FLUSH=50000;
  const size_t rowCap=FLUSH+(size_t)ndays;
  vector<int32_t> days(rowCap),sess32(narrow?rowCap:0),evs32(narrow?rowCap:0);vector<int64_t> sess(narrow?0:rowCap),evs(narrow?0:rowCap),rev(rowCap);
  vector<uint8_t> catbits((rowCap+7)/8,0);
  string uid(1<<16,'\0'),catv,catbody;size_t uidLen=0,nrows=0;
  char* uidp=uid.data();size_t uidCap=uid.size();
  int32_t* const dayp=days.data();int32_t* const sess32p=sess32.data();int32_t* const evs32p=evs32.data();int64_t* const sessp=sess.data();int64_t* const evsp=evs.data();int64_t* const revp=rev.data();
  auto flush_rows=[&]{
   if(!nrows)return;
   // top_category page: 1-bit definition levels as one bit-packed run, then the non-null values.
   const size_t n=nrows,groups=(n+7)/8;
   catbody.clear();
   string head;put_varint(head,(groups<<1)|1);
   put_le32(catbody,(uint32_t)(head.size()+groups));catbody+=head;catbody.append((const char*)catbits.data(),groups);catbody+=catv;
   if(!file.is_open())open_file();
   file.row_group((int64_t)n,{string_view(uid.data(),uidLen),string_view((const char*)days.data(),n*4),
    narrow?string_view((const char*)sess32.data(),n*4):string_view((const char*)sess.data(),n*8),
    narrow?string_view((const char*)evs32.data(),n*4):string_view((const char*)evs.data(),n*8),
    string_view((const char*)rev.data(),n*8),catbody});
   memset(catbits.data(),0,groups);catv.clear();uidLen=0;nrows=0;
  };
  Scratch<Ev> bufA,bufB;Scratch<Click> bufC,bufD;
  vector<uint32_t> thist((size_t)maxDigit+2),phist((size_t)W+3),chist((size_t)W+2);
  uint32_t* const th=thist.data();uint32_t* const pp=phist.data();uint32_t* const cp=chist.data();
  #pragma omp for schedule(dynamic,1)
  for(size_t up=0;up<UPn;++up){
   size_t nE=0,nC=0;
   for(int t=0;t<T;++t){nE+=surv[t].ev[up].size();nC+=surv[t].cl[up].size();}
   if(!nE)continue;
   const uint32_t base=(uint32_t)(up<<pshift);
   // Counting passes: by cell (lowest digit first), then by person. The first pass reads the per-thread
   // lists directly, so the events are never gathered separately.
   const unsigned sh0=tpass[0].sh;const uint32_t m0=tpass[0].mask;
   memset(th,0,((size_t)m0+2)*sizeof(uint32_t));memset(pp,0,((size_t)W+3)*sizeof(uint32_t));
   for(int t=0;t<T;++t){const EvList& L=surv[t].ev[up];
    for(const EvList::Block* bl=L.head;bl;bl=bl->next){const Ev* s=bl->items;const size_t m=L.count(bl);
     #pragma GCC unroll 4
     for(size_t i=0;i<m;++i){++th[((s[i].k>>sh0)&m0)+1];++pp[(s[i].person-base)+2];}}}
   for(uint32_t d=0;d<m0;++d)th[d+1]+=th[d];
   // pp[i+1] = where person i's events start; after the person pass pp[i]..pp[i+1] is person i's range.
   // nE counts tombstones too.
   for(uint32_t i=0;i<W;++i){
    if(pp[i+2]&&base+i<totalIds)__builtin_prefetch(&personNames[base+i],0,1); // names are needed when rows are written
    pp[i+2]+=pp[i+1];
   }
   pp[W+2]+=pp[W+1]; // tombstones (person = base+W) are placed after everybody and never looked at
   Ev* A=bufA.get(nE);Ev* B=bufB.get(nE);
   for(int t=0;t<T;++t){const EvList& L=surv[t].ev[up];
    for(const EvList::Block* bl=L.head;bl;bl=bl->next){const Ev* s=bl->items;const size_t m=L.count(bl);
     #pragma GCC unroll 4
     for(size_t i=0;i<m;++i){const __m128i e=_mm_loadu_si128((const __m128i*)(s+i));_mm_storeu_si128((__m128i*)(A+th[((uint64_t)_mm_cvtsi128_si64(e)>>sh0)&m0]++),e);}}}
   for(size_t q=1;q<tpass.size();++q){
    const unsigned sh=tpass[q].sh;const uint32_t mk=tpass[q].mask;
    memset(th,0,((size_t)mk+2)*sizeof(uint32_t));
    for(size_t i=0;i<nE;++i)++th[((A[i].k>>sh)&mk)+1];
    for(uint32_t d=0;d<mk;++d)th[d+1]+=th[d];
    for(size_t i=0;i<nE;++i){const __m128i e=_mm_loadu_si128((const __m128i*)(A+i));_mm_storeu_si128((__m128i*)(B+th[((uint64_t)_mm_cvtsi128_si64(e)>>sh)&mk]++),e);}
    swap(A,B);
   }
   {uint32_t* const pq=pp+1;
    #pragma GCC unroll 4
    for(size_t i=0;i<nE;++i){const __m128i e=_mm_loadu_si128((const __m128i*)(A+i));_mm_storeu_si128((__m128i*)(B+pq[A[i].person-base]++),e);}}
   const Ev* const E=B;
   // Clicks of people with events, grouped by person the same way.
   Click* C=nullptr;
   if(nC){
    const unsigned csh0=cpass[0].sh;const uint32_t cm0=cpass[0].mask;
    memset(cp,0,((size_t)W+2)*sizeof(uint32_t));memset(th,0,((size_t)cm0+2)*sizeof(uint32_t));
    for(int t=0;t<T;++t){const ClickList& L=surv[t].cl[up];
     for(const ClickList::Block* bl=L.head;bl;bl=bl->next){const Click* s=bl->items;const size_t m=L.count(bl);
      #pragma GCC unroll 4
      for(size_t i=0;i<m;++i){const uint32_t x=s[i].person-base;const uint32_t keep=pp[x]!=pp[x+1];cp[x+2]+=keep;th[((s[i].key>>csh0)&cm0)+1]+=keep;}}}
    for(uint32_t d=0;d<cm0;++d)th[d+1]+=th[d];
    for(uint32_t i=0;i<W;++i)cp[i+2]+=cp[i+1];
    const size_t nK=cp[W+1]; // clicks kept
    Click* CA=bufC.get(nC);Click* CB=bufD.get(nC);
    for(int t=0;t<T;++t){const ClickList& L=surv[t].cl[up];
     for(const ClickList::Block* bl=L.head;bl;bl=bl->next){const Click* s=bl->items;const size_t m=L.count(bl);
      #pragma GCC unroll 4
      for(size_t i=0;i<m;++i){const uint32_t x=s[i].person-base;if(pp[x]!=pp[x+1])memcpy((void*)(CA+th[(s[i].key>>csh0)&cm0]++),(const void*)(s+i),sizeof(Click));}}}
    for(size_t q=1;q<cpass.size();++q){
     const unsigned sh=cpass[q].sh;const uint32_t mk=cpass[q].mask;
     memset(th,0,((size_t)mk+2)*sizeof(uint32_t));
     for(size_t i=0;i<nK;++i)++th[((CA[i].key>>sh)&mk)+1];
     for(uint32_t d=0;d<mk;++d)th[d+1]+=th[d];
     for(size_t i=0;i<nK;++i)memcpy((void*)(CB+th[(CA[i].key>>sh)&mk]++),(const void*)(CA+i),sizeof(Click));
     swap(CA,CB);
    }
    {uint32_t* const cq=cp+1;
     #pragma GCC unroll 4
     for(size_t i=0;i<nK;++i)memcpy((void*)(CB+cq[CA[i].person-base]++),(const void*)(CA+i),sizeof(Click));}
    C=CB;
   }
   for(uint32_t pi=0;pi<W;++pi){
    const size_t es=pp[pi],ee=pp[pi+1];if(es==ee)continue;
    const uint32_t person=base+pi;
    const Click *cb=nullptr,*ce=nullptr;
    if(nC){
     Click* c0=C+cp[pi];Click* c1=C+cp[pi+1];const size_t n=(size_t)(c1-c0);
     if(n>1){
      // The clicks are already in cell order; finish each cell exactly.
      if(n<=24)click_insertion_sort(c0,c1);
      else for(Click* r0=c0;r0<c1;){
       const uint64_t cellEnd=((r0->key>>CCSH)+1)<<CCSH;Click* r1=r0+1;while(r1<c1&&r1->key<cellEnd)++r1;
       if(r1-r0>1){if(r1-r0<=24)click_insertion_sort(r0,r1);else sort(r0,r1,click_before);}
       r0=r1;
      }
     }
     cb=c0;ce=c1;
    }
    const Click *add=cb,*expire=cb;
    int currentday=-1;uint64_t dsess=0,devs=0,drev=0;
    Totals* cur=nullptr;bool purchasing=false;uint64_t prevmax=0;
    for(size_t a=es;;){
     // One cell: earliest and latest time, and whether it holds a purchase. Events are ordered by cell,
     // so the cell ends at the first key at or past its upper bound.
     uint64_t mn=0,mx=0,any=0;size_t b=a;bool fresh=false;int d=currentday;
     if(a<ee){
      mn=E[a].k;mx=mn;any=mn;const uint64_t cellEnd=((mn>>CSH)+1)<<CSH;
      for(b=a+1;b<ee;++b){const uint64_t x=E[b].k;if(x>=cellEnd)break;mn=min(mn,x);mx=max(mx,x);any|=x;}
      // A session starts with this cell if it is the person's first or the gap to the previous cell is too long.
      // Its day is the UTC date of its first event.
      if(!cur||(mn>>EV_TS)-prevmax>(uint64_t)GAP){fresh=true;d=(int)(((mn>>EV_TS)+dayOff)/(uint64_t)DAY);}
     }
     if(a==ee||d!=currentday){
      if(currentday>=0){
       // The (person, day) row is complete.
       uint16_t top=0;uint64_t best=0;
       if(!touched.empty()){
        for(uint16_t c:touched){if(catsum[c]>best||(catsum[c]==best&&catsum[c]>0&&catnames[c]<catnames[top])){top=c;best=catsum[c];}catsum[c]=0;}
        touched.clear();
       }
       const size_t r=nrows++;
       // user_id: names of up to 15 bytes sit inside their PName and are copied as one fixed 16-byte block.
       if(uidCap-uidLen<64){uid.resize(uid.size()*2);uidp=uid.data();uidCap=uid.size();}
       unsigned char inl=0xFF;if(person<totalIds)inl=(unsigned char)personNames[person].b[15];
       if(inl!=0xFF){char* o=uidp+uidLen;const uint32_t L=inl;memcpy(o,&L,4);memcpy(o+4,personNames[person].b,16);uidLen+=4+L;}
       else{const string_view name=name_of(person);const uint32_t L=(uint32_t)name.size();
        if(uidCap-uidLen<(size_t)L+64){uid.resize(max(uid.size()*2,uidLen+L+64));uidp=uid.data();uidCap=uid.size();}
        char* o=uidp+uidLen;memcpy(o,&L,4);memcpy(o+4,name.data(),L);uidLen+=4+L;}
       dayp[r]=(int32_t)(firstday+currentday);revp[r]=(int64_t)drev;
       if(narrow){sess32p[r]=(int32_t)dsess;evs32p[r]=(int32_t)devs;}else{sessp[r]=(int64_t)dsess;evsp[r]=(int64_t)devs;}
       if(best!=0){catbits[r>>3]|=(uint8_t)(1u<<(r&7));put_bytes(catv,catnames[top]);}
       if(debugCsv){string& o=dbg[tid];const string_view name=name_of(person);
        o+=hexstr(name);o+=',';o+=to_string(firstday+currentday);o+=',';o+=to_string(dsess);o+=',';o+=to_string(devs);o+=',';o+=to_string(drev);o+=',';
        o+=best==0?string("NULL"):hexstr(catnames[top]);o+='\n';
       }
       dsess=0;devs=0;drev=0;
      }
      if(a==ee)break;
      currentday=d;
     }
     if(fresh){
      const uint64_t tmin=mn>>EV_TS;
      ++dsess;
      // Sliding seven-day window, inclusive on both ends.
      const uint64_t addLim=(tmin+(uint64_t)WEEK+1)<<16,expLim=tmin<<16;
      while(add<ce&&add->key<addLim){const uint16_t c=(uint16_t)add->key;if(freq[c]++==0){positions[c]=(uint32_t)nact;active[nact++]=c;}++add;}
      while(expire<add&&expire->key<expLim){const uint16_t c=(uint16_t)expire->key;if(--freq[c]==0){const uint32_t pos=positions[c];const uint16_t back=active[--nact];active[pos]=back;positions[back]=pos;}++expire;}
      uint16_t credited=organic;
      if(add>expire)credited=(uint16_t)add[-1].key;
      cur=&tot[(size_t)d*ncamp+credited];cur->sessions++;purchasing=false;
      uint64_t* const arow=assists.data()+(size_t)d*ncamp;
      #pragma GCC unroll 4
      for(size_t q=0;q<nact;++q)++arow[active[q]];
      ++num_sessions;
     }
     prevmax=mx>>EV_TS;devs+=b-a;
     if(any&EV_PURCHASE){
      for(size_t q=a;q<b;++q){const uint64_t x=E[q].k;
       if(x&EV_PURCHASE){const uint64_t price=x&EV_PRICE;const uint16_t cat=E[q].cat;
        drev+=price;cur->revenue+=price;
        if(!purchasing){purchasing=true;cur->purchasing_sessions++;}
        if(catsum[cat]==0)touched.push_back(cat);
        catsum[cat]+=price;
       }
      }
     }
     a=b;
    }
    for(size_t q=0;q<nact;++q)freq[active[q]]=0;
    nact=0;
    if(nrows>=FLUSH)flush_rows();
   }
  }
  flush_rows();
  if(tid==0&&!file.is_open())open_file(); // the folder always holds at least one file, even with no rows
  // Every bucket is done (the loop above ends with a barrier): each thread hands back its own survivor
  // lists here, in parallel, instead of leaving all of it to process exit.
  if((size_t)tid<surv.size())surv[(size_t)tid].pool.release();
  if(file.is_open())file.close();
 }
 vector<Totals> campTotals(ncamp*(size_t)ndays);
 for(const auto& tt:threadTotals)for(size_t i=0;i<tt.size();++i){campTotals[i].sessions+=tt[i].sessions;campTotals[i].purchasing_sessions+=tt[i].purchasing_sessions;campTotals[i].revenue+=tt[i].revenue;}
 for(const auto& ta:threadAssists)for(size_t i=0;i<ta.size();++i)campTotals[i].assists+=ta[i];
__mark("AGGREGATE");
 {
  string cid,chan,csv;vector<int32_t> days;vector<int64_t> s,p,r,a;
  for(size_t ci=0;ci<ncamp;++ci)for(int d=0;d<ndays;++d){const auto& t=campTotals[(size_t)d*ncamp+ci];if(t.sessions==0&&t.assists==0)continue;
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
 // glibc grows each thread's heap with one mprotect per small step, and every mprotect takes the
 // process-wide mapping lock that all page faults need. Grow in 64 MB steps and never shrink, so the
 // parser threads do not queue behind each other in the kernel.
 mallopt(M_TOP_PAD,64<<20);
 mallopt(M_TRIM_THRESHOLD,INT_MAX);
 mallopt(M_MMAP_THRESHOLD,32<<20);
 const char* d=getenv("DATA_DIR");const char* s=getenv("SCRATCH_DIR");const char* o=getenv("OUTPUT_DIR");
 run(d?d:"/data",s?s:"/scratch",o?o:"/output");return 0;
 }catch(const exception& e){cerr<<"ERROR "<<e.what()<<'\n';return 1;}}
