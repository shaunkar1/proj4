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
#include <deque>
#include <cstddef>
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
// All files of one input folder in a single reservation: each file starts at a page boundary and is
// followed by at least one page of zeros (as in map_file). One mmap per file instead of two, and the
// whole folder can be unmapped later in a few large pieces.
struct FileGroup{char* base=nullptr;size_t len=0;vector<size_t> ends;}; // ends: offset just past each mapped file's slot
static vector<Mapped> map_files(const vector<fs::path>& paths,FileGroup& g){
 vector<size_t> lens(paths.size(),0);size_t total=0;
 for(size_t i=0;i<paths.size();++i){
  struct stat st{};if(stat(paths[i].c_str(),&st)!=0)throw runtime_error("Cannot stat "+paths[i].string());
  lens[i]=(size_t)st.st_size;if(lens[i])total+=((lens[i]+4095)&~size_t(4095))+4096;
 }
 vector<Mapped> out(paths.size());
 if(!total)return out;
 void* base=mmap(nullptr,total,PROT_READ,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
 if(base==MAP_FAILED)throw runtime_error("Cannot reserve address space for the input files");
 g.base=(char*)base;g.len=total;size_t off=0;
 static const bool willNeed=!getenv("SS_NOWILLNEED"); // measuring switch
 for(size_t i=0;i<paths.size();++i){
  if(!lens[i])continue;
  int fd=open(paths[i].c_str(),O_RDONLY);
  if(fd<0)throw runtime_error("Cannot open "+paths[i].string());
  void* got=mmap(g.base+off,lens[i],PROT_READ,MAP_PRIVATE|MAP_FIXED,fd,0);
  close(fd);
  if(got==MAP_FAILED)throw runtime_error("Cannot map "+paths[i].string());
  if(willNeed)madvise(got,lens[i],MADV_WILLNEED);
  out[i]={g.base+off,lens[i]};
  off+=((lens[i]+4095)&~size_t(4095))+4096;g.ends.push_back(off);
 }
 return out;
}
// Cuts a group into about `parts` pieces of similar size at file boundaries (for unmapping in parallel).
static void group_pieces(const FileGroup& g,size_t parts,vector<pair<char*,size_t>>& out){
 size_t start=0,k=1;
 for(size_t e:g.ends){
  if(e>=g.len/parts*k||e==g.len){if(e>start)out.push_back({g.base+start,e-start});start=e;while(k<parts&&g.len/parts*k<=e)++k;}
 }
 if(start<g.len)out.push_back({g.base+start,g.len-start});
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
// Measured on the scoring machine: huge pages help, bulk pre-faulting and the side-thread unmap do not.
static bool optHuge=true,optPrefault=false,optAsyncFree=false;
// A fresh anonymous mapping of at least `bytes`: zero-filled, touched lazily, 2 MB aligned and (unless
// SS_NOHUGE) advised for huge pages, so first touches cost one fault per 2 MB. The whole mapping
// (for munmap) is reported through rawBase/rawLen when asked for.
static void* map_huge(size_t bytes,void** rawBase=nullptr,size_t* rawLen=nullptr){
 const size_t huge=size_t(2)<<20;
 const size_t want=(max<size_t>(bytes,1)+huge-1)&~(huge-1);
 void* mem=mmap(nullptr,want+huge,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
 if(mem==MAP_FAILED)throw bad_alloc();
 char* a=(char*)(((uintptr_t)mem+huge-1)&~(uintptr_t)(huge-1));
 if(optHuge)madvise(a,want,MADV_HUGEPAGE);
 if(rawBase)*rawBase=mem;
 if(rawLen)*rawLen=want+huge;
 return a;
}
// Bump memory for one thread on such mappings. It grows by whole regions (first `first` bytes, then
// `next` bytes at a time), is never handed back piecemeal, and always starts zeroed because nothing is reused.
struct HugePool{
 char* cur=nullptr;char* lim=nullptr;size_t first=size_t(8)<<20,next=size_t(8)<<20;
 HugePool()=default;HugePool(const HugePool&)=delete;HugePool& operator=(const HugePool&)=delete;
 __attribute__((noinline,cold)) void grow(size_t bytes){
  const size_t huge=size_t(2)<<20;
  const size_t want=(max(bytes,cur?next:first)+huge-1)&~(huge-1);
  cur=(char*)map_huge(want);lim=cur+want;
 }
 void* take(size_t bytes){
  bytes=(bytes+63)&~size_t(63);
  if((size_t)(lim-cur)<bytes)grow(bytes);
  void* r=cur;cur+=bytes;return r;
 }
 void* take_page(size_t bytes){ // a multiple of 4096 bytes, starting at a 4096-byte boundary
  cur=(char*)(((uintptr_t)cur+4095)&~(uintptr_t)4095);
  if(cur>lim||(size_t)(lim-cur)<bytes)grow(bytes); // regions start 2 MB aligned
  void* r=cur;cur+=bytes;return r;
 }
};
struct TransparentHash {
 using is_transparent=void;
 size_t operator()(string_view x)const noexcept {return hash<string_view>{}(x);}
};
template <class V> using FastMap=unordered_map<string,V,TransparentHash,equal_to<>>;
// 64 -> 64 bit mixer: the full 128-bit product with an odd constant, high half folded onto the low half.
// Every output bit gets a well-mixed contributor: the product's top low-half bits and bottom high-half
// bits depend on all input bits. Used for table slots (low bits), bucket choice (top bits) and tags.
static inline uint64_t mix64(uint64_t x){const unsigned __int128 m=(unsigned __int128)x*0x9E3779B97F4A7C15ULL;return (uint64_t)m^(uint64_t)(m>>64);}
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
 Slot* slots=nullptr;size_t cap=0,mask=0,count=0;vector<Long> longs;void* slotsMap=nullptr;size_t slotsMapLen=0;
 // lock-free mode: new keys get idBase+0, idBase+1, ... up to idLimit keys; newNames keeps their text
 atomic<uint32_t> nextId{0};uint32_t idBase=0,idLimit=0;PName* newNames=nullptr;void* namesMap=nullptr;size_t namesMapLen=0;mutex longMu;
 explicit KeyTable(size_t want=1024){alloc(want);}
 KeyTable(const KeyTable&)=delete;KeyTable& operator=(const KeyTable&)=delete;
 ~KeyTable(){if(slotsMap)munmap(slotsMap,slotsMapLen);if(namesMap)munmap(namesMap,namesMapLen);}
 void alloc(size_t want){
  size_t c=1024;while(c<want)c<<=1;
  slots=(Slot*)map_huge(c*sizeof(Slot),&slotsMap,&slotsMapLen); // zero pages, touched lazily, huge where the kernel allows it
  cap=c;mask=c-1;
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
  Slot* old=slots;size_t oldcap=cap;void* oldMap=slotsMap;size_t oldMapLen=slotsMapLen;alloc(max(cap*2,want));
  if(!count){munmap(oldMap,oldMapLen);return;} // nothing to move: do not even touch the old pages
  for(size_t i=0;i<oldcap;++i){Slot& e=old[i];uint32_t c=e.k1.load(R);if(!c)continue;
   size_t j=(c==LONGK?e.k0.load(R):hash_packed(e.k0.load(R),c))&mask;
   while(slots[j].k1.load(R))j=(j+1)&mask;
   slots[j].k0.store(e.k0.load(R),R);slots[j].k1.store(c,R);slots[j].v.store(e.v.load(R),R);
  }
  munmap(oldMap,oldMapLen);
 }
 template<class FN> void remap(FN&& fn){ // rewrite every stored value
  const auto R=memory_order_relaxed;
  for(size_t i=0;i<cap;++i){Slot& e=slots[i];uint32_t c=e.k1.load(R);if(c&&c!=LONGK)e.v.store(fn(e.v.load(R)),R);}
  for(auto& l:longs)l.v=fn(l.v);
 }
 // ---- lock-free mode ----
 void begin_parallel(uint32_t base,uint32_t maxNew){
  idBase=base;idLimit=maxNew;nextId.store(0);
  // Straight from a lazily touched mapping: calloc would hand out heap memory and zero all of it here, on one thread.
  newNames=(PName*)map_huge(((size_t)maxNew+1)*sizeof(PName),&namesMap,&namesMapLen);
  if((count+maxNew)*5/4>cap)grow((count+maxNew)*5/4); // safety net: the caller sizes the table (at most 80% full)
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
 // Replaces the value of a key that is in the table (packed form, j = its home slot). Other threads may do the same for other keys.
 void set_packed(uint64_t k0,uint32_t k1,size_t j,uint32_t v){
  const auto R=memory_order_relaxed;
  for(;;j=(j+1)&mask){Slot& e=slots[j];const uint32_t c=e.k1.load(R);
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
 // The same with new IDs taken from a block of numbers reserved for the calling thread, so threads
 // adding many keys do not all write the shared counter (and neighbouring name/union-find entries)
 // for every key. Numbers left over in a block are simply never used.
 struct IdBlock{uint32_t next=0,end=0;};
 static constexpr uint32_t ID_BLOCK=64;
 uint32_t find_or_add_packed_block(uint64_t k0,uint32_t k1,size_t j,IdBlock& blk){
  const auto R=memory_order_relaxed;
  for(;;){
   Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
   if(c==k1){if(e.k0.load(R)==k0)return e.v.load(R);}
   else if(c==0){
    if(blk.next==blk.end){ // refill before claiming the slot
     const uint32_t b=nextId.fetch_add(ID_BLOCK,R);
     if(b>=idLimit)return NONE;
     blk.next=b;blk.end=idLimit-b<ID_BLOCK?idLimit:b+ID_BLOCK;
    }
    uint32_t expect=0;
    if(!e.k1.compare_exchange_strong(expect,BUSY,memory_order_acquire))continue;
    const uint32_t id=blk.next++;
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
// Append-only array of parsed records in the calling thread's arena. Like a vector, but appending is
// always inlined (it sits in the hottest loops) and nothing is ever value-initialised or freed singly.
template<class T> struct Bucket{
 T* b=nullptr;T* e=nullptr;T* c=nullptr;
 size_t size()const{return (size_t)(e-b);}
 size_t capacity()const{return (size_t)(c-b);}
 bool full()const{return e==c;}
 T* data(){return b;}
 const T* data()const{return b;}
 T& operator[](size_t i){return b[i];}
 const T& operator[](size_t i)const{return b[i];}
 [[gnu::noinline]] void reserve(size_t n){ // only the thread that owns the bucket may call this
  if(n<=capacity())return;
  T* const nb=(T*)tlArena.get(n*sizeof(T));const size_t have=size();
  if(have)memcpy((void*)nb,(const void*)b,have*sizeof(T));
  b=nb;e=nb+have;c=nb+n;
 }
// After each append the cache line after the one just written is requested: a bucket is one of many
 // slow write streams, too many for the hardware prefetcher, so without this every second record
 // waits for a line of fresh memory. (Measured: PARSE processor time -20% on dev and on 20M events.)
 // A prefetch never faults, so running past the end of the storage is harmless.
#ifdef SS_NO_PFNEXT
 __attribute__((always_inline)) static void want_next(const T*){}
#else
 __attribute__((always_inline)) static void want_next(const T* e){__builtin_prefetch((const char*)e+64,1,3);}
#endif
 __attribute__((always_inline)) void append(const T& r){*e++=r;want_next(e);} // there must be room
 __attribute__((always_inline)) void push_back(const T& r){if(__builtin_expect(e==c,0))reserve(max<size_t>(16,2*capacity()));*e++=r;want_next(e);}
};
[[maybe_unused]] static void unmap_regions(vector<Region> regions){for(const Region& r:regions)munmap(r.base,r.len);}
static constexpr uint32_t LOCAL_USER=0x80000000u;
// One parsed event line, 32 bytes (two per cache line), bucketed by event_id hash so duplicates meet in one bucket.
//  id    start of the event_id text in the mapped input. Its length is not kept: the text ends at the
//        first ',' or '"' (an ID holds neither, and field 0 is always followed by one of them).
//  k     0 for a line that is not a valid event, else Ev::k (see below) + PR_KBIAS: the relative time is
//        kept one higher, so a valid k is never 0.
//  user  the user/person number (filled in by resolve_users on the AVX2 path); may carry LOCAL_USER.
//  cat   category number of a purchase, else 0.
//  hx    hash bits 25-40 of the event_id.
//  ih    ingest code << 25 | hash bits 0-24. The ingest code is ingest_ts in milliseconds counted from
//        ingestBase (about 8.7 years before the window; 39 bits reach about 8.7 years past it).
//        INGEST_SIDE means "out of that range": the true value is then in the thread's ingestSide list
//        under this id pointer. Only the general parser path stores such records.
struct PRec{const char* id;uint64_t k;uint32_t user;uint16_t cat;uint16_t hx;uint64_t ih;};
static_assert(sizeof(PRec)==32&&offsetof(PRec,user)==16&&offsetof(PRec,cat)==20&&offsetof(PRec,hx)==22);
static constexpr uint64_t PR_KBIAS=1ull<<25; // = 1 << EV_TS; relative time + 1 still fits (window < 2^37 ms)
static constexpr unsigned PR_HBITS=25;
static constexpr uint64_t PR_HMASK=(1ull<<PR_HBITS)-1;
static constexpr uint64_t INGEST_SIDE=(1ull<<39)-1;
static int64_t ingestBase; // set with the window: window_start - 2^38 ms
// Ingest code of a timestamp in milliseconds, INGEST_SIDE if it is out of range. Codes order like the times.
static inline uint64_t ingest_code(int64_t ms){
 if(ms<ingestBase)return INGEST_SIDE;
 const uint64_t c=(uint64_t)ms-(uint64_t)ingestBase; // exact: the true distance is below 2^64
 return c<INGEST_SIDE?c:INGEST_SIDE;
}
// h = hkey(event_id); code = ingest code; k and cat as described above.
static inline __attribute__((always_inline)) PRec make_prec(const char* id,uint64_t h,uint64_t code,uint64_t k,unsigned cat,uint32_t user=0){
 PRec r;r.id=id;r.k=k;
 const uint64_t w=(uint64_t)user|((uint64_t)(uint16_t)cat<<32)|((h>>PR_HBITS)<<48);memcpy((void*)&r.user,&w,8); // user, cat, hx in one store
 r.ih=(code<<PR_HBITS)|(h&PR_HMASK);return r;
}
// True if the event IDs starting at a and b are the same text (each ends at its first ',' or '"').
// Both point into mapped inputs, which may be read a little past the end of the text.
static inline bool same_event_id(const char* a,const char* b){
 const __m128i vc=_mm_set1_epi8(','),vq=_mm_set1_epi8('"');
 for(;;a+=16,b+=16){
  const __m128i x=_mm_loadu_si128((const __m128i*)a),y=_mm_loadu_si128((const __m128i*)b);
  const unsigned ta=(unsigned)_mm_movemask_epi8(_mm_or_si128(_mm_cmpeq_epi8(x,vc),_mm_cmpeq_epi8(x,vq)));
  const unsigned tb=(unsigned)_mm_movemask_epi8(_mm_or_si128(_mm_cmpeq_epi8(y,vc),_mm_cmpeq_epi8(y,vq)));
  const unsigned ne=~(unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(x,y))&0xFFFFu;
  if(ta|tb){
   if(!ta||!tb)return false; // one ends in this block, the other goes on
   const unsigned ea=(unsigned)__builtin_ctz(ta),eb=(unsigned)__builtin_ctz(tb);
   return ea==eb&&(ne&((1u<<ea)-1))==0;
  }
  if(ne)return false;
 }
}
// One valid click line, bucketed by click_id hash.
// key is already Click::key: (ts - (window_start - 7 days))<<16 | campaign.
struct CRec{const char* id;uint64_t h;uint64_t key;uint32_t idlen;uint32_t user;};
static_assert(sizeof(CRec)==32);
struct Chunk{const char* base;size_t len,from,to;bool click;};
// One surviving event, 16 bytes. k = tsrel<<25 | purchase<<24 | price, with tsrel = ts - window_start
// (< 2^37 ms: the window is at most 1000 days) and price <= 10,000,000 < 2^24. person is the compact person number.
struct Ev{uint64_t k;uint32_t person;uint16_t cat;uint16_t pad;};
static_assert(offsetof(PRec,cat)==offsetof(PRec,user)+4&&offsetof(PRec,hx)==offsetof(PRec,user)+6&&offsetof(Ev,cat)==offsetof(Ev,person)+4&&sizeof(Ev)==16); // dedup copies user+cat(+tag) as one word
static constexpr uint64_t EV_PURCHASE=1ull<<24,EV_PRICE=EV_PURCHASE-1;
static constexpr unsigned EV_TS=25;
static_assert(PR_KBIAS==1ull<<EV_TS);
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
// A person's events into time order (ties in any order). They usually are in order or nearly so:
// insertion, and a real sort only when that has moved more than a few places per event.
static inline void ev_time_sort(Ev* e0,Ev* e1){
 size_t budget=8*(size_t)(e1-e0)+64;
 for(Ev* x=e0+1;x<e1;++x){
  if(x->k<x[-1].k){
   const Ev v=*x;Ev* y=x;do{*y=y[-1];--y;}while(y>e0&&v.k<y[-1].k);*y=v;
   const size_t moved=(size_t)(x-y);
   if(moved>budget){sort(e0,e1,[](const Ev& a,const Ev& b){return a.k<b.k;});return;}
   budget-=moved;
  }
 }
}
// Scratch array living in a thread's HugePool. reserve() the largest size up front where it is known;
// get() beyond that takes a new, larger piece and abandons the old one.
template<class T> struct Scratch{
 T* p=nullptr;size_t cap=0;HugePool* pool=nullptr;
 void reserve(size_t n){if(n>cap){cap=n;p=(T*)pool->take(cap*sizeof(T));}}
 T* get(size_t n){if(n>cap)reserve(n+n/4+64);return p;}
};
// Append-only list of 4096-byte blocks at 4096-byte boundaries: nothing is ever copied when it grows,
// an element's address stays valid, and (block count at the time, index in the block) names an element.
template<class T,size_t BYTES> struct Blocks{
 static constexpr size_t PER=(BYTES-16)/sizeof(T);
 struct Block{Block* next;Block* prev;T items[PER];};
 static_assert(BYTES==4096&&sizeof(Block)<=BYTES&&PER<256);
 T* cur=nullptr;T* end=nullptr;Block* head=nullptr;Block* tail=nullptr;size_t nblocks=0;
 __attribute__((noinline,cold)) void grow(HugePool& pool){
  Block* b=(Block*)pool.take_page(BYTES);
  b->next=nullptr;b->prev=tail;if(tail)tail->next=b;else head=b;
  tail=b;++nblocks;cur=b->items;end=b->items+PER;
 }
 T* slot(HugePool& pool){if(cur==end)grow(pool);return cur++;} // the place for the next element
 size_t size()const{return nblocks?(nblocks-1)*PER+(size_t)(cur-tail->items):0;}
 size_t count(const Block* b)const{return b==tail?(size_t)(cur-b->items):PER;}
 // 32-bit name of the element just returned by slot(), and the way back from it.
 uint32_t mark(const T* p)const{return (uint32_t)(nblocks<<8)|(uint32_t)((((uintptr_t)p&(BYTES-1))-16)/sizeof(T));}
 T* at_mark(uint32_t m)const{Block* b=tail;for(size_t k=nblocks-(m>>8);k;--k)b=b->prev;return &b->items[m&255];}
};
using EvList=Blocks<Ev,4096>;using ClickList=Blocks<Click,4096>;
struct DRec;struct DCRec; // dense rows in bucket form, defined with the dense route below
struct alignas(64) ThreadState{
 SmallTable users; // IDs that did not fit in the shared table
 vector<pair<const char*,int64_t>> ingestSide; // (event id pointer, ingest ms) of records whose ingest code is INGEST_SIDE
 vector<Bucket<PRec>> eb;vector<Bucket<CRec>> cb; // parsed lines by event/click ID hash
 uint32_t tid=0; // index of this thread's state (a LOCAL_USER number only means something together with it)
 deque<array<char,16>> rawNames; // texts of raw users that ended up with a LOCAL_USER number (they need a place that stays)
 vector<Bucket<DRec>> db;vector<Bucket<DCRec>> dcb; // dense rows in bucket form, by counter range (SS_DENSE_RECORDS=bucket)
 uint64_t slotWaits=0; // eslotWaits of this thread
 uint32_t evEnd=0,clEnd=0; // one past the highest event / click counter this thread wrote a slot for
 uint32_t denseMax=0; // largest DENSE_USER code this thread produced (reported with dense_note_index when its parsing is done)
 string dumpRows; // SS_DENSE_DUMP only
 vector<AVec<Ev>> uev;vector<AVec<Click>> ucl; // survivors by person
};
// ---- Dense route, users (DENSE.md stages 2 and 4) ----
// These declarations need nothing of the dn:: block; the three functions that do are defined after run().
// True once the links stage has decided that user IDs may take the dense route (keys found, table limit below DENSE_USER).
static bool denseUsers=false;
// Per user index: how its aliases 0-7 are grouped by the simple link lines. Three bits per alias give
// the smallest alias of its group (so a word is always "flat": no chains to follow). The word is
// stored XOR the identity, so untouched zero memory means "every alias alone", and the stored field
// of alias a is a XOR its group's smallest alias.
static atomic<uint32_t>* denseComp=nullptr; // limI words on a lazily touched mapping
static constexpr uint32_t COMP_IDENT=0xFAC688u; // alias a at bits 3a..3a+2
static inline uint32_t comp_root(uint32_t w,uint32_t a){return (w>>(3*a))&7;} // w: a word XOR COMP_IDENT
static void comp_union(uint64_t idx,uint32_t a,uint32_t b){
 atomic<uint32_t>& c=denseComp[idx];uint32_t s=c.load(memory_order_relaxed);
 for(;;){
  const uint32_t w=s^COMP_IDENT,ra=comp_root(w,a),rb=comp_root(w,b);
  if(ra==rb)return;
  // Every field that holds hi gets lo, all eight at once: `eq` has the lowest bit of each such field.
  const uint32_t lo=min(ra,rb),hi=max(ra,rb),F=0x249249u,x=w^(hi*F),eq=~(x|(x>>1)|(x>>2))&F,w2=w^((hi^lo)*eq);
  if(c.compare_exchange_weak(s,w2^COMP_IDENT,memory_order_relaxed))return;
 }
}
// index<<3|alias -> index<<3|smallest alias of its group.
static inline uint32_t dense_root(uint32_t d){return d^((denseComp[d>>3].load(memory_order_relaxed)>>(3*(d&7)))&7);}
// Highest user index handed out as DENSE_USER so far. It sizes the person number space after parsing,
// so whoever produces DENSE_USER codes must report their indexes here before the parse stage ends
// (every index, or a thread's running maximum once per chunk); a code above it stops the run.
static atomic<uint32_t> denseMaxIdx{0};
static inline void dense_note_index(uint32_t idx){
 uint32_t cur=denseMaxIdx.load(memory_order_relaxed);
 while(idx>cur&&!denseMaxIdx.compare_exchange_weak(cur,idx,memory_order_relaxed)){}
}
// letter + canonical base 36 of y (y < 2^40) into out, which needs room for 9 bytes; returns the length.
// Same result as dn::text36, without the digit loop: four table lookups of two digits each.
static constexpr array<char,2592> PAIR36=[]{array<char,2592> a{};const char* dg="0123456789abcdefghijklmnopqrstuvwxyz";
 for(int i=0;i<1296;++i){a[2*i]=dg[i/36];a[2*i+1]=dg[i%36];}return a;}();
// The digits of y as eight bytes, first digit in the lowest byte, zero bytes after the last; n = how many.
static inline uint64_t digits36(uint64_t y,unsigned& n){
 const uint32_t hi=(uint32_t)(y/1679616u),lo=(uint32_t)(y%1679616u); // 36^4
 char buf[8];memcpy(buf,&PAIR36[2*(hi/1296u)],2);memcpy(buf+2,&PAIR36[2*(hi%1296u)],2);memcpy(buf+4,&PAIR36[2*(lo/1296u)],2);memcpy(buf+6,&PAIR36[2*(lo%1296u)],2);
 n=1+(y>=36u)+(y>=1296u)+(y>=46656u)+(y>=1679616u)+(y>=60466176u)+(y>=2176782336ull)+(y>=78364164096ull);
 uint64_t v;memcpy(&v,buf,8);return v>>(8*(8-n)); // drops the leading zeros
}
// A number that orders values (y < 2^40) the way their base-36 texts order byte-wise: the digits
// moved up to eight places, then the number of digits (a text sorts before every longer one it starts).
static constexpr uint64_t POW36[9]={1ull,36ull,1296ull,46656ull,1679616ull,60466176ull,2176782336ull,78364164096ull,2821109907456ull};
static inline uint64_t order36(uint64_t y){
 const unsigned n=1+(y>=36u)+(y>=1296u)+(y>=46656u)+(y>=1679616u)+(y>=60466176u)+(y>=2176782336ull)+(y>=78364164096ull);
 return y*POW36[8-n]*16+n;
}
static inline size_t text36_fast(char letter,uint64_t y,char* out){
 unsigned n;const uint64_t v=digits36(y,n);
 out[0]=letter;memcpy(out+1,&v,8);return (size_t)n+1;
}
// ---- Dense route, the reader of the slot arrays (DENSE stage) ----
// What the reader knows about a user index: where its events and clicks sit in the slot arrays
// (ef/el, cf/cl: first and last counter + 1, 0 = none; ec/cc: how many), whether any of its rows
// went through the general lane instead, and what was decided for it.
struct DInfo{atomic<uint32_t> ef,el,ec,cf,cl,cc;atomic<uint8_t> general,state;uint8_t pad[6];};
static_assert(sizeof(DInfo)==32);
static DInfo* denseInfo=nullptr; // highest reported user index + 2 entries, zero pages until touched
static constexpr uint8_t DSTATE_DEFER=1; // rows are handed to the general lane by the sweep after the index pass
// first-counter fields hold counter + 1, so that zero memory means "none".
static inline void dense_note_first(atomic<uint32_t>& first,uint32_t ctr1){
 uint32_t cur=first.load(memory_order_relaxed);
 while((cur==0||ctr1<cur)&&!first.compare_exchange_weak(cur,ctr1,memory_order_relaxed)){}
}
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
// ---- Dense IDs ----------------------------------------------------------------------------------
// Data made by the track's generator names every event, click and user by a counter pushed through a
// fixed bijection on 0..2^40 with one secret key per kind and written in base 36 behind a letter:
//   text = letter + base36( ((index*M1 ^ key) mod 2^40, x ^= x>>17, x*M2 mod 2^40) ).
// Undoing that turns an ID back into its counter, and counters are dense (0..N-1) and ordered: a user's
// events are consecutive event counters in time order, a user's clicks consecutive click counters, and
// a user ID is (user index)*512 + alias. Code that knows the counter can index arrays directly instead
// of hashing, and finds a person's rows next to each other.
// Nothing here may be needed for correctness: an ID takes the dense route only if its text is the
// canonical base-36 form (so text <-> number is one-to-one) and its counter is below a limit; every
// other ID keeps the general route, and a general ID can never equal a dense one as text. If the keys
// cannot be found (other data), `dense` stays off and the whole run is the general route.
namespace dn{
static constexpr uint64_t MASK=(1ull<<40)-1;
static constexpr uint64_t M1=0x9E3779B97F4Bull&MASK,M2=0xD6E8FEB86659ull&MASK;
static constexpr uint64_t inv40(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&MASK;}
static constexpr uint64_t I1=inv40(M1),I2=inv40(M2);
static_assert(((M1*I1)&MASK)==1&&((M2*I2)&MASK)==1);
static constexpr unsigned GEN_ALIAS_BITS=9; // user number = index<<9 | alias in the generator
static constexpr unsigned ALIAS_BITS=3;     // aliases 0..7 take the dense route (99% of rows); compact user = index<<3 | alias
// Undoes the multiplication by M2 and the xor-shift; the key and M1 are still in the result.
static inline __attribute__((always_inline)) uint64_t unmix(uint64_t y){const uint64_t m=(y*I2)&MASK;return m^(m>>17)^(m>>34);}
// Counter of the ID whose base-36 value is y (y < 2^40), and back.
static inline __attribute__((always_inline)) uint64_t index_of(uint64_t y,uint64_t key){return ((unmix(y)^key)*I1)&MASK;}
static inline uint64_t value_of(uint64_t index,uint64_t key){uint64_t m=((index*M1)^key)&MASK;m^=m>>17;return (m*M2)&MASK;}
// Canonical base 36: 1-8 characters of [0-9a-z], no leading zero unless it is the only digit, value < 2^40.
// (s, n) is the text after the ID's letter. Reference version; the parser may use its own vector form.
static inline bool parse36(const char* s,size_t n,uint64_t& y){
 if(n-1>7)return false;
 if(s[0]=='0'&&n>1)return false;
 uint64_t v=0;
 for(size_t i=0;i<n;++i){unsigned d=(unsigned char)s[i]-(unsigned)'0';if(d>9){d=(unsigned char)s[i]-(unsigned)'a';if(d>25)return false;d+=10;}v=v*36+d;}
 if(v>MASK)return false;
 y=v;return true;
}
// Writes letter + canonical base 36 of y into out (room for 9 bytes); returns the length.
static inline size_t text36(char letter,uint64_t y,char* out){
 char buf[8];int at=8;do{const unsigned d=(unsigned)(y%36);buf[--at]=(char)(d<10?'0'+d:'a'+(d-10));y/=36;}while(y);
 out[0]=letter;memcpy(out+1,buf+at,(size_t)(8-at));return (size_t)(9-at);
}
// Looks for the key under which sample 0 has a counter that is a multiple of `step` below `limit` and
// all but at most one of the other samples have counters below `limit`. um[i] = unmix(value of sample i).
// Candidates are tried in increasing counter order by all threads; the smallest hit wins.
static bool find_key(const uint64_t* um,size_t n,uint64_t limit,uint64_t step,uint64_t& key,int T){
 if(n<6||limit<step)return false;
 const uint64_t cand=limit/step;atomic<uint64_t> best{UINT64_MAX};
 const uint64_t inc=(M1*step)&MASK;
 #pragma omp parallel num_threads(T)
 {
  const int t=omp_get_thread_num();
  const uint64_t BL=1u<<16; // blocks are dealt round-robin so every thread stays near the low counters
  for(uint64_t b=(uint64_t)t*BL;b<cand&&b<best.load(memory_order_relaxed);b+=(uint64_t)T*BL){
   const uint64_t e=min(cand,b+BL);uint64_t a=(b*inc)&MASK;
   for(uint64_t c=b;c<e;++c,a=(a+inc)&MASK){
    const uint64_t k=um[0]^a;
    if((((um[1]^k)*I1)&MASK)>=limit&&(((um[2]^k)*I1)&MASK)>=limit)continue; // two misses already
    size_t miss=0;for(size_t i=1;i<n&&miss<2;++i)miss+=(((um[i]^k)*I1)&MASK)>=limit;
    if(miss<2){uint64_t cur=best.load(memory_order_relaxed);while(c<cur&&!best.compare_exchange_weak(cur,c)){}break;}
   }
  }
 }
 const uint64_t c=best.load();if(c==UINT64_MAX)return false;
 key=um[0]^((c*inc)&MASK);return true;
}
struct Info{
 bool events=false,clicks=false; // dense route usable for events / for clicks (each also needs the user key)
 uint64_t ke=0,ku=0,kc=0;        // keys of event, user and click IDs (low 40 bits)
 uint64_t limE=0,limC=0,limI=0;  // an event / click counter or a user index at or above its limit keeps the general route
};
// First `want` canonical IDs with the given letter found in column `col` at the start of a CSV file (header skipped).
static vector<uint64_t> sample_ids(const char* base,size_t len,int col,char letter,size_t want){
 vector<uint64_t> out;const char* p=(const char*)memchr(base,'\n',len);if(!p)return out;++p;
 const char* const end=base+len;
 while(p<end&&out.size()<want){
  const char* e=(const char*)memchr(p,'\n',(size_t)(end-p));if(!e)break;
  const char* f=p;int c=0;
  while(c<col&&f<e){const char* q=(const char*)memchr(f,',',(size_t)(e-f));if(!q){f=e;break;}f=q+1;++c;}
  if(c==col&&f<e){const char* q=(const char*)memchr(f,',',(size_t)(e-f));const char* g=q?q:e;
   if(g-f>=2&&*f=='"'&&g[-1]=='"'){++f;--g;}
   uint64_t y;if(g-f>=2&&*f==letter&&parse36(f+1,(size_t)(g-f-1),y))out.push_back(y);}
  p=e+1;
 }
 return out;
}
// Tries to find one key: any of the first samples may be the anchor (for users it must be an alias-0 ID).
static bool infer(const vector<uint64_t>& ys,uint64_t limit,uint64_t step,uint64_t& key,int T){
 if(ys.size()<12)return false;
 vector<uint64_t> um(ys.size());for(size_t i=0;i<ys.size();++i)um[i]=unmix(ys[i]);
 for(size_t anchor=0;anchor<6;++anchor){
  swap(um[0],um[anchor]);const bool ok=find_key(um.data(),um.size(),limit,step,key,T);swap(um[0],um[anchor]);
  if(!ok)continue;
  size_t in=0;for(uint64_t y:ys)in+=index_of(y,key)<limit; // most samples must agree, and the mapping must round-trip
  if(in*4>=ys.size()*3&&value_of(index_of(ys[0],key),key)==ys[0])return true;
 }
 return false;
}
}
static dn::Info dense; // set once before the links stage, read-only after that
// Records and shared state of the dense route (see DENSE.md).
static constexpr uint32_t DENSE_USER=0x40000000u; // user field: DENSE_USER | (index<<3 | alias) of a user index that is not complex; table IDs stay below this
// Dense events and clicks do not go through buckets: the parse stage writes each one straight into a slot
// indexed by its counter, so the slot arrays are the deduplicated tables when parsing is done.
// Event slot. k: 0 empty, 1 busy (a thread is writing the slot), 2 a line that is not a valid event (it
// still decides against copies that arrived later), anything else the k of a valid event in the PRec
// form (Ev::k + PR_KBIAS, never below 1 << 25). user, cat as in PRec; ingest = exact ingest_ts in ms;
// thread = index of the writing thread's state, needed to read a LOCAL_USER number in `user`.
struct ESlot{atomic<uint64_t> k;uint32_t user;uint16_t cat;uint16_t thread;int64_t ingest;uint64_t spare;};
static_assert(sizeof(ESlot)==32);
static constexpr uint64_t ESLOT_BUSY=1,ESLOT_INVALID=2;
// Click slot (valid clicks only). present: 0 empty, else index of the writing thread's state + 1.
// key as in CRec. Copies of a click are identical, so every writer stores the same key, and user/present
// go out as one 8-byte store (a LOCAL_USER number and its thread stay together).
struct CSlot{uint64_t key;uint32_t user;uint32_t present;};
static_assert(sizeof(CSlot)==16);
static ESlot* denseEv=nullptr; // dense.limE slots, zero pages; allocated before the parse stage when dparse.events
static CSlot* denseCl=nullptr; // dense.limC slots; when dparse.clicks
// One past the highest event / click counter that has a slot written (0: none), for whoever reads the slots.
// Every thread reports its own value when its parsing is done (like dense_note_index).
static atomic<uint32_t> denseEvEnd{0},denseClEnd{0};
static inline void dense_note_end(atomic<uint32_t>& end,uint32_t n){
 uint32_t cur=end.load(memory_order_relaxed);
 while(n>cur&&!end.compare_exchange_weak(cur,n,memory_order_relaxed)){}
}
// One row into its event slot: the copy with the smallest ingest_ts stays, whatever the order and the threads
// the copies come from. (Copies with equal ingest_ts are identical in defined data; the first one stays.)
// kk = the row's k, or ESLOT_INVALID for a line that is not a valid event.
static thread_local uint64_t eslotWaits=0; // times this thread found a slot busy or lost the race for it (SS_STATS; only the rare paths count)
// uct = user, cat and thread as the eight bytes they occupy in the slot (eslot_uct): one store instead of three.
static inline __attribute__((always_inline)) uint64_t eslot_uct(uint32_t user,unsigned cat,unsigned thread){return (uint64_t)user|((uint64_t)(uint16_t)cat<<32)|((uint64_t)thread<<48);}
static_assert(offsetof(ESlot,user)==8&&offsetof(ESlot,cat)==12&&offsetof(ESlot,thread)==14&&offsetof(ESlot,ingest)==16);
// Raw users (dparse.raw). On a dense-record event line the vector loops do not decode the user ID: if the
// user text is 'u' + 0 to 8 bytes, the slot gets
//   user  = DENSE_USER_RAW | number of bytes after the 'u' (0-8)       (top two bits set: no table number,
//           no DENSE_USER code and no LOCAL_USER number looks like this)
//   spare = those bytes as they stand in the file, zero-padded to a 64-bit word (first byte lowest)
// and any other user text is resolved in the loop as before. Text and code are one-to-one, so nothing is
// lost; resolve_raw_users() (in run(), after all lines are parsed and before the ID table closes) turns
// every raw slot into what the loop would have written: a DENSE_USER code if the text is a dense user
// (canonical, alias < 8, index below the limit and not complex), else the table number of the text, or a
// LOCAL_USER number of the resolving thread, with `thread` set to that thread. It also reports the highest
// dense index (dense_note_index). After it no slot is raw, and spare is not looked at again. Whoever folds
// this into another pass over the slots has to do exactly that, before tab.end_parallel() if table IDs can
// still be new. Only slots with a valid event (k >= PR_KBIAS) carry a user at all.
static constexpr uint32_t DENSE_USER_RAW=0xC0000000u;
static inline __attribute__((always_inline)) void eslot_commit(ESlot& s,uint64_t kk,uint64_t uct,int64_t ingest,uint64_t spare=0){
 uint64_t cur=s.k.load(memory_order_relaxed);
 for(;;){
  if(__builtin_expect(cur==ESLOT_BUSY,0)){++eslotWaits;_mm_pause();cur=s.k.load(memory_order_relaxed);continue;}
  if(__builtin_expect(!s.k.compare_exchange_weak(cur,ESLOT_BUSY,memory_order_acquire,memory_order_relaxed),0)){++eslotWaits;continue;} // cur is reloaded
  if(cur==0||ingest<s.ingest){memcpy((char*)&s+offsetof(ESlot,user),&uct,8);s.ingest=ingest;s.spare=spare;s.k.store(kk,memory_order_release);}
  else s.k.store(cur,memory_order_release);
  return;
 }
}
static inline __attribute__((always_inline)) void cslot_commit(CSlot& s,uint64_t key,uint32_t user,uint32_t thread){
 const uint64_t w=(uint64_t)user|((uint64_t)(thread+1)<<32);
 __atomic_store_n(&s.key,key,__ATOMIC_RELAXED);memcpy((void*)&s.user,&w,8);
}
// Bit per user index: set in the links stage when one of the index's IDs is linked to anything but a
// dense alias 0-7 of the same index. IDs of a complex index resolve through the ID table like general IDs.
static uint64_t* denseComplex=nullptr; // limI bits, zero-filled; allocated when dense.events or dense.clicks
static inline __attribute__((always_inline)) bool dense_complex(uint64_t idx){return (denseComplex[idx>>6]>>(idx&63))&1;}
static inline bool dense_set_complex(uint64_t idx){return (__atomic_fetch_or(&denseComplex[idx>>6],1ull<<(idx&63),__ATOMIC_RELAXED)>>(idx&63))&1;} // returns whether it was set already
// Decodes a user ID text: true if it is canonical 'u' + base 36 with a user index below the limit (any alias).
static inline bool dense_decode_user(const char* s,size_t n,uint64_t& idx,uint32_t& alias){
 uint64_t y;
 if(n<2||s[0]!='u'||!dn::parse36(s+1,n-1,y))return false;
 const uint64_t num=dn::index_of(y,dense.ku);
 idx=num>>dn::GEN_ALIAS_BITS;alias=(uint32_t)(num&((1u<<dn::GEN_ALIAS_BITS)-1));
 return idx<dense.limI;
}
// ---- Dense route, parse stage (DENSE.md stage 3) ----
// Which dense records the parse stage produces; set once in run() before the parse stage.
//   users   a dense user ID with alias 0-7 of a non-complex index becomes DENSE_USER | (index<<3 | alias)
//   events  an event line with a dense event_id goes to its slot denseEv[counter] (eslot_commit)
//   clicks  a valid click line with a dense click_id goes to its slot denseCl[counter]
// Dense records come in two forms (SS_DENSE_RECORDS):
//   slot    the parse stage writes the slot itself. The vector loops ask for the slot's memory when the ID is
//           decoded and write it at the end of the line, or `ahead` dense rows later (SS_DENSE_AHEAD, 1-16).
//   bucket  the row is appended to a per-thread bucket by counter range (counter >> shE / shC); a later pass
//           fills the slot arrays bucket by bucket, so the random writes stay inside a small range.
struct DenseParse{bool users=false,events=false,clicks=false,bucket=false,raw=false,twoPhase=false;unsigned ahead=8,shE=14,shC=14;};
// A dense event line in bucket form, valid or not. k as in PRec (0 = not a valid event), ingest = exact
// ingest_ts in ms, lo = the low shE bits of the counter: counter = (bucket index << shE) | lo.
struct DRec{uint64_t k;int64_t ingest;uint32_t user;uint16_t cat;uint16_t lo;};
static_assert(sizeof(DRec)==24);
// A valid click line in bucket form. kc = counter, key as in CRec.
struct DCRec{uint64_t key;uint32_t kc;uint32_t user;};
static_assert(sizeof(DCRec)==16);
// A dense row waiting for its slot (slot form with ahead > 0). uct overlays nothing else: the user is final when
// the row enters. A free place points at a scratch slot of the loop, so leaving the ring needs no test.
static constexpr unsigned RING_MAX=16;
struct ERing{uint64_t k;int64_t ingest;uint64_t uct;uint64_t spare;ESlot* slot;};
struct CRing{uint64_t key;CSlot* slot;uint32_t user;uint32_t pad;};
static DenseParse dparse;
// The route of an ID depends on its text only. These are the definitions every path must agree with
// (the vector loops use parse36_v3, which gives the same answer as dn::parse36).
// Counter of the ID (s, n) if it is `letter` + canonical base 36 and the counter is below `limit`.
static inline bool dense_counter(const char* s,size_t n,char letter,uint64_t key,uint64_t limit,uint64_t& ctr){
 uint64_t y;
 if(n<2||s[0]!=letter||!dn::parse36(s+1,n-1,y))return false;
 ctr=dn::index_of(y,key);return ctr<limit;
}
// User code of a dense user counter (index<<9 | alias): only aliases 0-7 of an index that is below the limit and not complex.
static inline __attribute__((always_inline)) bool dense_user_code(uint64_t ctr,uint32_t& code){
 const uint64_t idx=ctr>>dn::GEN_ALIAS_BITS,alias=ctr&((1u<<dn::GEN_ALIAS_BITS)-1);
 if(alias>=(1u<<dn::ALIAS_BITS)||idx>=dense.limI||dense_complex(idx))return false;
 code=DENSE_USER|(uint32_t)((idx<<dn::ALIAS_BITS)|alias);return true;
}
// The user field of a line whose user_id is (s, n), if that is a dense user (scalar form, for the general handlers).
static inline bool dense_user(const char* s,size_t n,uint32_t& code){
 uint64_t ctr;
 return dense_counter(s,n,'u',dense.ku,dn::MASK+1,ctr)&&dense_user_code(ctr,code);
}
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
// month and day digits, so the dates of one year never push each other out. Only valid dates within
// reach of an ingest code (see PRec) are stored; for any other text the answer is false.
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
 const int64_t ms=days_from_civil(y,(unsigned)m,(unsigned)dd)*DAY;
 // Days that an ingest code cannot express (years away from the window) are not remembered and not
 // answered here: the callers then take their general route, which is exact for any date.
 if(ms<ingestBase||(uint64_t)ms-(uint64_t)ingestBase>=INGEST_SIDE-(uint64_t)DAY)return false;
 dayms=ms;
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
// dn::parse36 for the vector loops: canonical base 36 of the n bytes at s (which may be read up to 8).
// The text is moved so that its last character is the top byte of a word and the bytes below its first
// character are '0'; then all eight are checked and turned into digits at once and combined with two
// multiply-adds. "No leading zero" is the same as "the value has n digits", i.e. value >= 36^(n-1); that and
// value < 2^40 are one unsigned compare with per-length constants (lengths 0 and 9+ can never pass it).
// The vector constants are written with different halves on purpose: a constant of sixteen equal bytes is
// rebuilt in a register on every use (three instructions each), an uneven one is read from memory.
static constexpr uint64_t MIN36[10]={~0ULL,0,36ULL,1296ULL,46656ULL,1679616ULL,60466176ULL,2176782336ULL,78364164096ULL,~0ULL};
static constexpr uint64_t SPAN36[10]={0,dn::MASK,dn::MASK-36ULL,dn::MASK-1296ULL,dn::MASK-46656ULL,dn::MASK-1679616ULL,dn::MASK-60466176ULL,dn::MASK-2176782336ULL,dn::MASK-78364164096ULL,0};
SS_V3I bool parse36_v3(const char* s,unsigned n,uint64_t& y){
 const unsigned m=n<9u?n:9u;
 uint64_t w;memcpy(&w,s,8);
 const unsigned sh=(64u-8u*m)&63u; // 0..56 for lengths 1-8
 const uint64_t x=(w<<sh)|_bzhi_u64(0x3030303030303030ULL,sh);
 const __m128i v=_mm_cvtsi64_si128((long long)x); // upper half zero
 #define SS_LOW8(c) _mm_setr_epi8((char)(c),(char)(c),(char)(c),(char)(c),(char)(c),(char)(c),(char)(c),(char)(c),0,0,0,0,0,0,0,0)
 const __m128i t=_mm_add_epi8(v,SS_LOW8(-'0')),u=_mm_add_epi8(v,SS_LOW8(-'a'));
 // a byte is fine if it is 0-9 (t <= 9) or a-z (u <= 25): one of the two saturating differences is zero
 const __m128i bad=_mm_min_epu8(_mm_subs_epu8(t,SS_LOW8(9)),_mm_subs_epu8(u,SS_LOW8(25)));
 const __m128i d=_mm_min_epu8(t,_mm_add_epi8(v,SS_LOW8(10-'a'))); // the digit of a 0-9 or a-z byte; meaningless otherwise
 #undef SS_LOW8
 const __m128i p2=_mm_maddubs_epi16(d,_mm_setr_epi8(36,1,36,1,36,1,36,1,0,0,0,0,0,0,0,0));
 const __m128i p4=_mm_madd_epi16(p2,_mm_setr_epi16(1296,1,1296,1,0,0,0,0));
 const uint64_t r=(uint64_t)_mm_cvtsi128_si64(p4);
 const uint64_t val=(r&0xFFFFFFFFULL)*1679616ULL+(r>>32);
 y=val;
 return (bool)_mm_testz_si128(bad,bad)&(val-MIN36[m]<=SPAN36[m]);
}
// Two IDs at once (event_id or click_id in the low half, user_id in the high half): the digit arithmetic of
// parse36_v3 on sixteen bytes. Returns true if all sixteen bytes are base-36 digits; then text a is canonical
// and below 2^40 iff ya-MIN36[ma]<=SPAN36[ma], and likewise b (parse36x2_ok). If it returns false one of the
// texts has another byte (or a length the padding cannot handle) and the caller decides each text on its own.
// The constants are read through a pointer the compiler cannot see through, for the reason given above
// (here all sixteen bytes are equal).
alignas(16) static const uint8_t P36K[7][16]={
 {208,208,208,208,208,208,208,208,208,208,208,208,208,208,208,208}, // -'0'
 {159,159,159,159,159,159,159,159,159,159,159,159,159,159,159,159}, // -'a'
 {9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9},
 {25,25,25,25,25,25,25,25,25,25,25,25,25,25,25,25},
 {169,169,169,169,169,169,169,169,169,169,169,169,169,169,169,169}, // 10-'a'
 {36,1,36,1,36,1,36,1,36,1,36,1,36,1,36,1},
 {0x10,0x05,1,0,0x10,0x05,1,0,0x10,0x05,1,0,0x10,0x05,1,0}};       // 16-bit words 1296,1
SS_V3I bool parse36x2_v3(const char* sa,unsigned na,const char* sb,unsigned nb,uint64_t& ya,uint64_t& yb,unsigned& ma,unsigned& mb){
 ma=na<9u?na:9u;mb=nb<9u?nb:9u;
 uint64_t wa,wb;memcpy(&wa,sa,8);memcpy(&wb,sb,8);
 const unsigned sha=(64u-8u*ma)&63u,shb=(64u-8u*mb)&63u;
 const uint64_t xa=(wa<<sha)|_bzhi_u64(0x3030303030303030ULL,sha),xb=(wb<<shb)|_bzhi_u64(0x3030303030303030ULL,shb);
 const __m128i* K=(const __m128i*)P36K;__asm__("":"+r"(K));
 const __m128i v=_mm_insert_epi64(_mm_cvtsi64_si128((long long)xa),(long long)xb,1);
 const __m128i t=_mm_add_epi8(v,_mm_load_si128(K)),u=_mm_add_epi8(v,_mm_load_si128(K+1));
 const __m128i bad=_mm_min_epu8(_mm_subs_epu8(t,_mm_load_si128(K+2)),_mm_subs_epu8(u,_mm_load_si128(K+3)));
 const __m128i d=_mm_min_epu8(t,_mm_add_epi8(v,_mm_load_si128(K+4)));
 const __m128i p4=_mm_madd_epi16(_mm_maddubs_epi16(d,_mm_load_si128(K+5)),_mm_load_si128(K+6));
 const uint64_t ra=(uint64_t)_mm_cvtsi128_si64(p4),rb=(uint64_t)_mm_extract_epi64(p4,1);
 ya=(ra&0xFFFFFFFFULL)*1679616ULL+(ra>>32);yb=(rb&0xFFFFFFFFULL)*1679616ULL+(rb>>32);
 return (bool)_mm_testz_si128(bad,bad);
}
static inline __attribute__((always_inline)) bool parse36x2_ok(uint64_t y,unsigned m){return y-MIN36[m]<=SPAN36[m];}
// ---- Links stage helpers ----
// The two IDs of a link line as dense user IDs: da / db say whether each is one (canonical 'u' + base 36,
// user index below the limit; any alias), ia / ib and aa / ab are their user indexes and aliases.
struct LinkDense{uint64_t ia,ib;uint32_t aa,ab;bool da,db;};
static void link_dense_base(const char* s0,size_t l0,const char* s1,size_t l1,LinkDense& r){
 r.da=dense_decode_user(s0,l0,r.ia,r.aa);r.db=dense_decode_user(s1,l1,r.ib,r.ab);
}
// The same answer with both texts decoded in one vector pass (the usual line: two dense IDs).
SS_V3 static void link_dense_v3(const char* s0,size_t l0,const char* s1,size_t l1,LinkDense& r){
 uint64_t ya=0,yb=0;unsigned ma,mb;bool da=false,db=false;
 if(__builtin_expect(parse36x2_v3(s0+1,(unsigned)min<size_t>(l0-1,9),s1+1,(unsigned)min<size_t>(l1-1,9),ya,yb,ma,mb),1)){da=s0[0]=='u'&&parse36x2_ok(ya,ma);db=s1[0]=='u'&&parse36x2_ok(yb,mb);}
 else{da=s0[0]=='u'&&parse36_v3(s0+1,(unsigned)min<size_t>(l0-1,9),ya);db=s1[0]=='u'&&parse36_v3(s1+1,(unsigned)min<size_t>(l1-1,9),yb);}
 const uint64_t na=dn::index_of(ya,dense.ku),nb=dn::index_of(yb,dense.ku);
 r.ia=na>>dn::GEN_ALIAS_BITS;r.aa=(uint32_t)(na&((1u<<dn::GEN_ALIAS_BITS)-1));r.da=da&&r.ia<dense.limI;
 r.ib=nb>>dn::GEN_ALIAS_BITS;r.ab=(uint32_t)(nb&((1u<<dn::GEN_ALIAS_BITS)-1));r.db=db&&r.ib<dense.limI;
}
// One link line starting at p: its two fields, and where the next line starts. False if the line does not
// have exactly two non-empty fields (it is skipped). The usual shape, under 32 bytes with one comma and
// each field bare or wrapped in quotes, is cut from two 16-byte compares; the general splitter decides everything else.
static inline __attribute__((always_inline)) bool link_split(const char* p,const char* end,string_view* f,const char*& next){
 if(end-p>=32){
  const __m128i vnl=_mm_set1_epi8('\n'),vc=_mm_set1_epi8(','),vq=_mm_set1_epi8('"');
  const __m128i x0=_mm_loadu_si128((const __m128i*)p),x1=_mm_loadu_si128((const __m128i*)(p+16));
  const uint32_t mnl=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vnl))|((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vnl))<<16);
  if(mnl){
   const unsigned L=(unsigned)__builtin_ctz(mnl);const uint32_t keep=(1u<<L)-1;
   const uint32_t mc=((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vc))|((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vc))<<16))&keep;
   const uint32_t mq=((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vq))|((uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vq))<<16))&keep;
   if(mc&&!(mc&(mc-1))){
    // One comma. Quotes may only wrap a whole field: q0 / q1 say whether each field starts with one,
    // and then it must also end with one and no other quote may be in the line.
    const unsigned P=(unsigned)__builtin_ctz(mc);
    const uint32_t q0=mq&1u,q1=(mq>>(P+1))&1u;
    const uint32_t want=q0|(q0<<(P-1+(P==0)))|(q1<<(P+1))|(q1<<(L-1+(L==0)));
    if(__builtin_expect(mq==want&&P>=2*q0&&L-P-1>=2*q1,1)){
     next=p+L+1;
     f[0]=string_view(p+q0,P-2*q0);f[1]=string_view(p+P+1+q1,L-P-1-2*q1);
     return P>2*q0&&L-P-1>2*q1;
    }
   }
  }
 }
 const char* comma[2];int ncomma,nquote;const char* lineEnd;
 next=scan_line(p,end,comma,2,ncomma,nquote,lineEnd);
 return take_fields(p,lineEnd,comma,ncomma,nquote,2,f)&&!f[0].empty()&&!f[1].empty();
}
// A pushed record that still needs its user: the packed user key, its table slot, and the record.
// A bucket is only ever grown right after the waiting records have been finished (see the capacity
// checks), so `rec` cannot be left pointing into storage that has moved.
struct PendUser{uint64_t k0;const char* u;uint32_t* up;uint32_t j;uint32_t k1;}; // up: the record's user field; j fits: the table has at most 2^32 slots
static constexpr int V3_BATCH=16;
static void resolve_users(const PendUser* pend,const PendUser* pendEnd,KeyTable& tab,ThreadState& tl){
 const auto R_=memory_order_relaxed;
 KeyTable::Slot* const slots=tab.slots;
 for(;pend!=pendEnd;++pend){
  const PendUser& q=*pend;KeyTable::Slot& e=slots[q.j];uint32_t g;
  if(__builtin_expect(e.k1.load(memory_order_acquire)==q.k1&&e.k0.load(R_)==q.k0,1))g=e.v.load(R_);
  else{
   const string_view u(q.u,(size_t)(q.k1>>24)-1);
   g=tab.find_or_add_packed(q.k0,q.k1,q.j,u);
   if(g==KeyTable::NONE)g=LOCAL_USER|tl.users.intern(u);
  }
  *q.up=g;
 }
}
// ---- The line loops, once per way of building the window masks ----
// The masks of a 64-byte window (bit i = byte i is a comma / a quote / a newline) are the only thing
// that differs between the vector paths:
//   avx2     two 32-byte compares per character and movemask (the default)
//   avx512z  one 64-byte compare per character straight into a mask register (AVX-512BW)
//   avx512y  two 32-byte compares per character into mask registers, joined with kunpckdq
//            (AVX-512BW+VL; no 512-bit register is touched, so no 512-bit frequency licence)
// Everything after the masks is the same code (event_line_v3 / click_line_v3), inlined into each loop.
// The AVX-512 loops are only compiled with 256-bit vectors as the compiler's own choice, so the one
// 512-bit use is the explicit window load and compare of avx512z.
struct WinMasks{uint64_t c,q,n;};
SS_V3I WinMasks win_masks_avx2(const char* p){
 const __m256i vnl=_mm256_set1_epi8('\n'),vc=_mm256_set1_epi8(','),vq=_mm256_set1_epi8('"');
 const __m256i x0=_mm256_loadu_si256((const __m256i*)p),x1=_mm256_loadu_si256((const __m256i*)(p+32));
 return {mask64_v3(x0,x1,vc),mask64_v3(x0,x1,vq),mask64_v3(x0,x1,vnl)};
}
#define SS_V5 __attribute__((target("avx2,bmi,bmi2,popcnt,lzcnt,avx512f,avx512bw,avx512vl,prefer-vector-width=256")))
#define SS_V5I SS_V5 static inline __attribute__((always_inline))
SS_V5I WinMasks win_masks_avx512z(const char* p){
 const __m512i x=_mm512_loadu_si512((const void*)p);
 return {_mm512_cmpeq_epi8_mask(x,_mm512_set1_epi8(',')),_mm512_cmpeq_epi8_mask(x,_mm512_set1_epi8('"')),_mm512_cmpeq_epi8_mask(x,_mm512_set1_epi8('\n'))};
}
SS_V5I WinMasks win_masks_avx512y(const char* p){
 const __m256i vnl=_mm256_set1_epi8('\n'),vc=_mm256_set1_epi8(','),vq=_mm256_set1_epi8('"');
 const __m256i x0=_mm256_loadu_si256((const __m256i*)p),x1=_mm256_loadu_si256((const __m256i*)(p+32));
 return {_cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(x1,vc),_mm256_cmpeq_epi8_mask(x0,vc))),
         _cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(x1,vq),_mm256_cmpeq_epi8_mask(x0,vq))),
         _cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(x1,vnl),_mm256_cmpeq_epi8_mask(x0,vnl)))};
}
// Newlines only, for the line index (see SS_EVENT_LOOP_IX).
SS_V3I uint64_t nl_mask_avx2(const char* p){
 const __m256i vnl=_mm256_set1_epi8('\n');
 return mask64_v3(_mm256_loadu_si256((const __m256i*)p),_mm256_loadu_si256((const __m256i*)(p+32)),vnl);
}
SS_V5I uint64_t nl_mask_avx512z(const char* p){return _mm512_cmpeq_epi8_mask(_mm512_loadu_si512((const void*)p),_mm512_set1_epi8('\n'));}
SS_V5I uint64_t nl_mask_avx512y(const char* p){
 const __m256i vnl=_mm256_set1_epi8('\n');
 return _cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(_mm256_loadu_si256((const __m256i*)(p+32)),vnl),_mm256_cmpeq_epi8_mask(_mm256_loadu_si256((const __m256i*)p),vnl)));
}
// A line the short path passed on: the portable splitter and the general handler decide. Kept out of
// line and compiled without any vector extension, so the loops stay small and no wide register is used here.
template<int N,class GEN> [[gnu::noinline]] static const char* line_general(const char* p,const char* end,GEN&& general){
 const char* comma[N];int ncomma,nquote;const char* lineEnd;string_view f[N];
 const char* const next=scan_line(p,end,comma,N,ncomma,nquote,lineEnd);
 if(take_fields(p,lineEnd,comma,ncomma,nquote,N,f))general(f);
 return next;
}
// Where the first data line of a chunk starts and where the chunk's lines stop; false if there is none.
static inline bool chunk_lines(const char* base,size_t len,size_t from,size_t to,const char*& p,const char*& lim){
 if(from>=len)return false;
 const char* nl=(const char*)memchr(from?base+from-1:base,'\n',from?len-(from-1):len);
 if(!nl)return false;
 p=nl+1;lim=base+min(to,len);return true;
}
// What an event loop keeps across lines (never changes during a chunk).
struct EvLoop{ThreadState& tl;KeyTable& tab;const KeyTable& products;size_t PE;const char* end;DateMemo3* memo;int64_t wstart;uint64_t wlen,ibase;Bucket<PRec>* buckets;KeyTable::Slot* slots;size_t tmask;
 bool dUsers;uint64_t ke,ku,limE;ESlot* ev;uint64_t tid48;bool bucket,raw;unsigned ahead,shE;uint32_t loMask;Bucket<DRec>* db; // dense route (dparse); tid48 = thread index << 48 (eslot_uct)
 EvLoop(ThreadState& tl_,KeyTable& tab_,const KeyTable& products_,size_t PE_,const char* end_):tl(tl_),tab(tab_),products(products_),PE(PE_),end(end_),memo(date_memo3()),
  wstart(window_start),wlen((uint64_t)(window_end-window_start)),ibase((uint64_t)ingestBase),buckets(tl_.eb.data()),slots(tab_.slots),tmask(tab_.mask),
  dUsers(dparse.users),ke(dense.ke),ku(dense.ku),limE(dense.limE),ev(denseEv),tid48((uint64_t)tl_.tid<<48),bucket(dparse.bucket),raw(dparse.raw),ahead(dparse.ahead),shE(dparse.shE),loMask((1u<<dparse.shE)-1),db(tl_.db.data()){}
};
// Start of window 2 of an event line: one past the third comma of window 1 (p+65 if there is none).
SS_V3I const char* event_window2(const char* p,uint64_t c){return p+_tzcnt_u64(_blsr_u64(_blsr_u64(c)))+1;}
// One event line at p, given the masks of its two windows (w1 at p, w2 at b=event_window2(p,w1.c)).
// Does exactly what for_records<7> + the general event handler would do with the line; `general` is
// that handler and gets the line if the short path passes it on. Returns the start of the next line.
static constexpr uint32_t NOPRE=0xFFFFFFFFu; // event_pre_v3: the line's event_id is not dense
template<bool DREC,bool PRE,class GEN> SS_V3I const char* event_line_v3(const char* const p,const char* const b,const WinMasks w1,const WinMasks w2,const char* const nl,const uint32_t pre,const EvLoop& lp,PendUser* const pend,PendUser*& pp,uint32_t& dmax,uint32_t& send,ERing* const ring,unsigned& rpos,GEN&& general){
 ThreadState& tl=lp.tl;KeyTable& tab=lp.tab;const KeyTable& products=lp.products;const size_t PE=lp.PE;
 DateMemo3* const memo=lp.memo;const int64_t wstart=lp.wstart;const uint64_t wlen=lp.wlen,ibase=lp.ibase;
 Bucket<PRec>* const buckets=lp.buckets;KeyTable::Slot* const slots=lp.slots;const size_t tmask=lp.tmask;
 {
  const char* next=nullptr;bool done=false;
  do{
   // Window 1: 64 bytes from the line start; must hold the first three commas (fields 0-2).
   const uint64_t c=w1.c,qa=w1.q;
   const uint64_t cA=_blsr_u64(c),cB=_blsr_u64(cA),cR=_blsr_u64(cB);
   const uint64_t m1=_blsmsk_u64(cB); // bytes up to and including the third comma
   const uint64_t q1=qa&m1,st1=((c^cB)<<1)|1,en1=c^cR;
   uint64_t sq1,op1;const uint64_t err1=quote_errors_v3(q1,st1,en1,sq1,op1);
   // Window 2: 64 bytes from the start of field 3; must hold the last three commas and the newline.
   const uint64_t e2=_tzcnt_u64(cB); // 64 if there is no third comma
   // The dense-record loops know where the line ends (nl, from the line index), so the windows need no newline
   // masks there: L = distance of the newline from the window start. A newline before the third comma makes L
   // negative, a line too long for the window makes it 64 or more; both fail the test below like a missing comma.
   uint64_t nbit,keep,L; // nbit: the newline's bit in window 2; keep: the bytes before it
   if(DREC){L=(uint64_t)(nl-b);nbit=1ull<<(L&63);keep=nbit-1;}
   else{const uint64_t nb=w2.n;L=_tzcnt_u64(nb);nbit=_blsi_u64(nb);keep=_blsmsk_u64(nb)>>1;} // L: 64 if there is no newline in the window
   const uint64_t d=w2.c&keep,qb=w2.q&keep;
   const uint64_t st2=(d<<1)|1,en2=d|nbit;
   uint64_t sq2,op2;const uint64_t err2=quote_errors_v3(qb,st2,en2,sq2,op2);
   // one test: quote errors, a newline before the third comma, not exactly three more commas, a missing comma or newline
   if(__builtin_expect((err1|err2|(DREC?0:(w1.n&m1))|(uint64_t)(_mm_popcnt_u64(d)^3)|((e2|L)>>6))!=0,0))break;
   next=DREC?nl+1:b+L+1;done=true; // from here on the line is well-formed, 7 fields
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
   const uint64_t icode=(uint64_t)ingest-ibase; // in range: date3 only knows days whose codes fit
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
   // Dense route (DENSE.md). DREC (dense records, which need dense users): both IDs are decoded in one pass;
   // a dense event_id sends the row to its slot (ask for the slot's memory right away), a dense user needs no table.
   uint64_t ve=0;bool dEv=false;uint32_t ucode=0;bool uDense=false;
   if(PRE){ve=pre;dEv=pre!=NOPRE;} // two-phase loop: decoded (and its slot asked for) a block of lines ago, see event_pre_v3
   else if(DREC&&lp.raw){ // raw users: only the event ID is decoded on the line
    uint64_t ye;
    if(__builtin_expect(s0[0]=='e'&&parse36_v3(s0+1,l0-1,ye)&&(ve=dn::index_of(ye,lp.ke))<lp.limE,1)){dEv=true;__builtin_prefetch(&lp.ev[ve],1,3);}
   }else if(DREC){
    uint64_t ye,yu;unsigned me,mu;
    if(__builtin_expect(parse36x2_v3(s0+1,l0-1,s1+1,l1-1,ye,yu,me,mu),1)){
     if(__builtin_expect(s0[0]=='e'&&parse36x2_ok(ye,me)&&(ve=dn::index_of(ye,lp.ke))<lp.limE,1)){dEv=true;if(!lp.bucket)__builtin_prefetch(&lp.ev[ve],1,3);}
     uDense=s1[0]=='u'&&parse36x2_ok(yu,mu)&&dense_user_code(dn::index_of(yu,lp.ku),ucode);
    }else{ // a byte that is no digit in one of the two texts: each on its own
     if(s0[0]=='e'&&parse36_v3(s0+1,l0-1,ye)&&(ve=dn::index_of(ye,lp.ke))<lp.limE){dEv=true;if(!lp.bucket)__builtin_prefetch(&lp.ev[ve],1,3);}
     uDense=s1[0]=='u'&&parse36_v3(s1+1,l1-1,yu)&&dense_user_code(dn::index_of(yu,lp.ku),ucode);
    }
   }
   const unsigned kind=event_kind(string_view(b+b4,l4));
   uint64_t k=0;unsigned cat=0; // k stays 0 unless the line is a valid event
   if(kind&&l1!=0&&has5){
    int64_t ts;bool tsOk;
    {
     uint64_t a8,b8;uint16_t a2,b2_;
     // An F1 ts on the same calendar day as ingest_ts (the usual case) needs no date work at all.
     if(l2==20&&s2[19]=='Z'&&(memcpy(&a8,s2,8),memcpy(&b8,s3,8),memcpy(&a2,s2+8,2),memcpy(&b2_,s3+8,2),((a8^b8)|(uint64_t)(a2^b2_))==0)){
      const int64_t tod2=tod_v3(s2);
      if(tod2>=0){ts=ingestDay+tod2;tsOk=true;}else tsOk=parse_ts(string_view(s2,l2),ts);
     }else tsOk=ts_v3(s2,l2,memo,ts);
    }
    // window_start <= ts < window_end as one unsigned compare (the window is far shorter than 2^63 ms)
    uint64_t tsrel;
    if(tsOk&&(tsrel=(uint64_t)ts-(uint64_t)wstart)<wlen&&ingest-ts<=DAY){
     int32_t price=0;const bool purchase=kind==3;
     if(purchase){
      const unsigned b5=(unsigned)_tzcnt_u64(B2),l5=(unsigned)_tzcnt_u64(E2)-b5;
      B2=_blsr_u64(B2);E2=_blsr_u64(E2);
      const unsigned b6=(unsigned)_tzcnt_u64(B2),l6=(unsigned)_tzcnt_u64(E2)-b6;
      if(price_value(string_view(b+b6,l6),price)){
       const uint32_t ci=products.find(string_view(b+b5,l5));
       cat=ci==KeyTable::NONE?0:(unsigned)(uint16_t)ci;k=((tsrel+1)<<EV_TS)|EV_PURCHASE|(uint64_t)(uint32_t)price;
      }
     }else k=(tsrel+1)<<EV_TS;
    }
   }
   bool tableUser=k!=0;
   if(DREC){
    // Dense records: the row goes straight into its slot. The slot's memory was requested when the ID was decoded
    // and has arrived by now (measured on the scoring machine: waiting longer changes nothing).
    uint64_t rawU=0;
    if(PRE||lp.raw){
     ucode=0;
     if(k){
      const unsigned n1=l1-1; // l1 >= 1 on a valid line
      if(__builtin_expect((s1[0]=='u')&(n1<=8u)&dEv,1)){ // left as text for resolve_raw_users (see DENSE_USER_RAW)
       uint64_t w;memcpy(&w,s1+1,8);rawU=_bzhi_u64(w,8*n1);ucode=DENSE_USER_RAW|n1;tableUser=false;
      }else if(dense_user(s1,l1,ucode)){tableUser=false;dmax=max(dmax,ucode);} // other shapes, and rows for the general buckets: decided here
     }
    }else if(k&&uDense){tableUser=false;dmax=max(dmax,ucode);}else ucode=0; // (no user in the record of a line that is not a valid event)
    uint32_t user=ucode;
    if(__builtin_expect(tableUser,0)){ // a general user (about 1% of the rows): through the ID table right here
     PendUser pu;
     if(__builtin_expect(!KeyTable::pack(string_view(s1,l1),pu.k0,pu.k1),0)){done=false;break;} // long user ID: nothing has been stored yet
     pu.j=(uint32_t)(KeyTable::hash_packed(pu.k0,pu.k1)&tmask);pu.u=s1;pu.up=&user;
     resolve_users(&pu,&pu+1,tab,tl);
    }
    if(__builtin_expect(dEv,1)){
     if(PRE){send=max(send,(uint32_t)ve+1);eslot_commit(lp.ev[ve],k?k:ESLOT_INVALID,(uint64_t)user|((uint64_t)cat<<32)|lp.tid48,ingest,rawU);}
     else if(lp.bucket){ // bucket form: appended by counter range, like a PRec
      Bucket<DRec>& vec=lp.db[ve>>lp.shE];
      if(__builtin_expect(vec.full(),0))vec.reserve(max<size_t>(16,2*vec.capacity()));
      DRec r;r.k=k;r.ingest=ingest;r.user=user;r.cat=(uint16_t)cat;r.lo=(uint16_t)((uint32_t)ve&lp.loMask);
      vec.append(r);
     }else{
      send=max(send,(uint32_t)ve+1);
      const uint64_t kk=k?k:ESLOT_INVALID,uct=(uint64_t)user|((uint64_t)cat<<32)|lp.tid48; // cat < 2^16
      if(lp.ahead){ // the row waits; the row it replaces has had `ahead` rows of time for its slot's memory to arrive
       ERing& e=ring[rpos];
       eslot_commit(*e.slot,e.k,e.uct,e.ingest,e.spare);
       e.k=kk;e.ingest=ingest;e.uct=uct;e.spare=rawU;e.slot=&lp.ev[ve];
       rpos=rpos+1==lp.ahead?0:rpos+1;
      }else eslot_commit(lp.ev[ve],kk,uct,ingest,rawU);
     }
    }else{ // an event_id that is not dense: a bucket record, as in the other loops
     const uint64_t h=hkey_v3(s0,l0);
     Bucket<PRec>& vec=buckets[(size_t)(((h>>40)*PE)>>24)];
     if(__builtin_expect(vec.full(),0))vec.reserve(max<size_t>(16,2*vec.capacity()));
     vec.append(make_prec(s0,h,icode,k,cat,user));
    }
   }else{
    if(lp.dUsers&&k){uint64_t y;if(s1[0]=='u'&&parse36_v3(s1+1,l1-1,y)&&dense_user_code(dn::index_of(y,lp.ku),ucode)){tableUser=false;dmax=max(dmax,ucode);}}
    uint64_t k0=0;uint32_t k1=0;
    if(tableUser&&__builtin_expect(!KeyTable::pack(string_view(s1,l1),k0,k1),0)){done=false;break;} // long user ID: nothing has been stored yet
    const uint64_t h=hkey_v3(s0,l0);
    const PRec r=make_prec(s0,h,icode,k,cat,ucode);
    Bucket<PRec>& vec=buckets[(size_t)(((h>>40)*PE)>>24)];
    if(__builtin_expect(vec.full(),0)){resolve_users(pend,pp,tab,tl);pp=pend;vec.reserve(max<size_t>(16,2*vec.capacity()));} // grow now: nothing waits on the old storage, and the record appended below stays put
    uint32_t* const up=&vec.e->user;vec.append(r);
    if(tableUser){
     const size_t j=KeyTable::hash_packed(k0,k1)&tmask;
     __builtin_prefetch(&slots[j],0,1);
     PendUser& pu=*pp++;pu.k0=k0;pu.j=(uint32_t)j;pu.u=s1;pu.up=up;pu.k1=k1;
     if(pp==pend+V3_BATCH){resolve_users(pend,pp,tab,tl);pp=pend;}
    }
   }
  }while(0);
  if(!done){
   // Keep the order of lines: finish the waiting ones before the general handler stores anything.
   resolve_users(pend,pp,tab,tl);pp=pend;
   next=line_general<7>(p,lp.end,general);
  }
  return next;
 }
}
// resolve_raw_users (see DENSE_USER_RAW), one thread's range of event slots. A raw text that is not a dense
// user goes through the ID table here (rare: other aliases, complex indexes, odd spellings).
[[gnu::noinline]] static uint32_t raw_user_table(uint64_t raw,uint32_t len,KeyTable& tab,ThreadState& tl){
 char tx[16]={'u',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};memcpy(tx+1,&raw,8);
 const string_view text(tx,len+1);
 uint32_t code=tab.find_or_add(text); // at most 9 bytes: the table keeps the bytes, not the pointer
 if(code==KeyTable::NONE){            // the table is full: a number of this thread, with a text that stays
  const auto it=tl.users.m.find(text);
  if(it!=tl.users.m.end())code=LOCAL_USER|it->second;
  else{tl.rawNames.push_back({});memcpy(tl.rawNames.back().data(),tx,16);code=LOCAL_USER|tl.users.intern(string_view(tl.rawNames.back().data(),len+1));}
 }
 return code;
}
// A user's events sit in neighbouring slots but under several aliases (texts), so a small table of the last
// texts seen saves most decodes. RAW_MEMO entries, indexed by a few bits of the text.
static constexpr unsigned RAW_MEMO=64;
struct RawMemo{uint64_t raw;uint32_t len,code;};
#define SS_RESOLVE_RAW(NAME,TARGET,PARSE36) \
TARGET static void NAME(ESlot* const ev,const size_t from,const size_t to,KeyTable& tab,ThreadState& tl,uint32_t& dmaxOut){ \
 RawMemo memo[RAW_MEMO];for(RawMemo& m:memo){m.raw=0;m.len=~0u;m.code=0;} \
 uint32_t dmax=0;const uint64_t ku=dense.ku;const uint16_t tid=(uint16_t)tl.tid; \
 for(size_t c=from;c<to;++c){ \
  ESlot& sl=ev[c]; \
  if(sl.k.load(memory_order_relaxed)<PR_KBIAS)continue; /* empty, or not a valid event: no user */ \
  const uint32_t u=sl.user; \
  if((u&DENSE_USER_RAW)!=DENSE_USER_RAW)continue;       /* resolved on its line already */ \
  const uint32_t len=u&15u;const uint64_t raw=sl.spare; \
  RawMemo& m=memo[(size_t)(((raw^len)*0x9E3779B97F4A7C15ULL)>>58)]; \
  if(__builtin_expect(m.raw!=raw||m.len!=len,0)){ \
   uint32_t code=0;uint64_t y; \
   if(PARSE36((const char*)&raw,len,y)&&dense_user_code(dn::index_of(y,ku),code))dmax=max(dmax,code); \
   else code=raw_user_table(raw,len,tab,tl); \
   m.raw=raw;m.len=len;m.code=code; \
  } \
  sl.user=m.code;sl.thread=tid; \
 } \
 dmaxOut=dmax; \
}
SS_RESOLVE_RAW(resolve_raw_v3,SS_V3,parse36_v3)
SS_RESOLVE_RAW(resolve_raw_plain,,dn::parse36)
// The event loop of one chunk for one mask builder.
#define SS_EVENT_LOOP(NAME,TARGET,MASKS) \
template<bool DREC,class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const EvLoop lp(tl,tab,products,PE,base+len);PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0; /* send: one past the highest counter written to a slot */ \
 ESlot scratch;scratch.k.store(0,memory_order_relaxed);ERing ring[RING_MAX];unsigned rpos=0;for(ERing& e:ring){e.k=ESLOT_INVALID;e.ingest=0;e.uct=0;e.spare=0;e.slot=&scratch;} /* dmax: largest dense user code seen */ \
 static_assert(!DREC,"the dense-record line code needs the line index"); \
 while(p<lim){const WinMasks w1=MASKS(p);const char* const b=event_window2(p,w1.c);p=event_line_v3<DREC,false>(p,b,w1,MASKS(b),nullptr,0,lp,pend,pp,dmax,send,ring,rpos,general);} \
 resolve_users(pend,pp,tab,tl);tl.denseMax=max(tl.denseMax,dmax); \
 for(const ERing& e:ring)eslot_commit(*e.slot,e.k,e.uct,e.ingest,e.spare); /* the rows still waiting (free places write the scratch slot) */ \
 tl.evEnd=max(tl.evEnd,send); \
}
// The same loop with a line index (SS_LINES=index): the newlines of the next IX_BLOCK bytes are located
// first, so where a line starts no longer waits for the previous line's fields to be cut - consecutive
// lines become independent work for the processor. p is always the first line not handled yet, and
// every newline before `scan` has been turned into a line start.
static constexpr size_t IX_BLOCK=1024;
#define SS_EVENT_LOOP_IX(NAME,TARGET,MASKS,NLMASK) \
template<bool DREC,class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const char* const end=base+len; \
 const EvLoop lp(tl,tab,products,PE,end);PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0; /* send: one past the highest counter written to a slot */ \
 ESlot scratch;scratch.k.store(0,memory_order_relaxed);ERing ring[RING_MAX];unsigned rpos=0;for(ERing& e:ring){e.k=ESLOT_INVALID;e.ingest=0;e.uct=0;e.spare=0;e.slot=&scratch;} /* dmax: largest dense user code seen */ \
 const char* starts[IX_BLOCK+64];const char* scan=p; \
 while(p<lim){ \
  size_t n=0; \
  for(const char* const stop=scan+IX_BLOCK;scan<stop&&scan<end;scan+=64){uint64_t m=NLMASK(scan);while(m){starts[n++]=scan+_tzcnt_u64(m)+1;m=_blsr_u64(m);}} \
  for(size_t i=0;i<n&&p<lim;++i){const WinMasks w1=MASKS(p);const char* const b=event_window2(p,w1.c);event_line_v3<DREC,false>(p,b,w1,MASKS(b),starts[i]-1,0,lp,pend,pp,dmax,send,ring,rpos,general);p=starts[i];} \
  if(scan>=end&&p<lim){ /* the last line has no newline */ \
   if(DREC){p=line_general<7>(p,end,general);break;} const WinMasks w1=MASKS(p);const char* const b=event_window2(p,w1.c);p=event_line_v3<DREC,false>(p,b,w1,MASKS(b),nullptr,0,lp,pend,pp,dmax,send,ring,rpos,general);break;} \
 } \
 resolve_users(pend,pp,tab,tl);tl.denseMax=max(tl.denseMax,dmax); \
 for(const ERing& e:ring)eslot_commit(*e.slot,e.k,e.uct,e.ingest,e.spare); /* the rows still waiting (free places write the scratch slot) */ \
 tl.evEnd=max(tl.evEnd,send); \
}
SS_EVENT_LOOP_IX(events_avx2_ix,SS_V3,win_masks_avx2,nl_mask_avx2)
SS_EVENT_LOOP_IX(events_avx512z_ix,SS_V5,win_masks_avx512z,nl_mask_avx512z)
SS_EVENT_LOOP_IX(events_avx512y_ix,SS_V5,win_masks_avx512y,nl_mask_avx512y)
SS_EVENT_LOOP(events_avx2,SS_V3,win_masks_avx2)
SS_EVENT_LOOP(events_avx512z,SS_V5,win_masks_avx512z)
SS_EVENT_LOOP(events_avx512y,SS_V5,win_masks_avx512y)
// Two-phase event loop (the default for dense records with raw users). The line index gives the starts of
// the next block of lines before any of them is parsed, so phase 1 runs over a whole block and only decodes
// each line's event_id and asks for its slot's memory (event_pre_v3); phase 2 is the usual line code with
// that counter handed in, writing the slot directly. Phase 1 of a block runs before phase 2 of the block
// before it, so every slot has had at least a block of lines (about 13) of time to arrive: no ring.
// The event_id of a well-formed line is what lies before the first comma, without the quotes of a quoted
// field; a line that is not well-formed never uses the result (it goes to the general handler).
SS_V3I uint32_t event_pre_v3(const char* const p,const EvLoop& lp){
 const unsigned cm=(unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128((const __m128i*)p),_mm_set1_epi8(',')));
 const unsigned c=(unsigned)_tzcnt_u32(cm|0x10000u); // the first comma; 16 if there is none that near (no dense ID is that long)
 const unsigned z=(unsigned)(p[0]=='"');
 const char* const s0=p+z;const unsigned l0=c-2*z;    // odd shapes give lengths that parse36_v3 refuses
 uint64_t y,ve;
 if(__builtin_expect(s0[0]=='e'&&parse36_v3(s0+1,l0-1,y)&&(ve=dn::index_of(y,lp.ke))<lp.limE,1)){__builtin_prefetch(&lp.ev[ve],1,3);return (uint32_t)ve;}
 return NOPRE;
}
struct IxBlock{const char* first;size_t n;const char* starts[IX_BLOCK+64];uint32_t pre[IX_BLOCK+64];}; // lines first, starts[0], ...; n newlines found
// Index a block starting at line FIRST (at least one newline unless the file ends first), then phase 1 on its lines.
#define SS_IX_FILL(B,FIRST,NLMASK) {(B).first=(FIRST);(B).n=0; \
 while((B).n==0&&scan<end){for(const char* const stop=scan+IX_BLOCK;scan<stop&&scan<end;scan+=64){uint64_t m=NLMASK(scan);while(m){(B).starts[(B).n++]=scan+_tzcnt_u64(m)+1;m=_blsr_u64(m);}}} \
 {const char* q_=(B).first;for(size_t i_=0;i_<(B).n;++i_){(B).pre[i_]=event_pre_v3(q_,lp);q_=(B).starts[i_];}}}
#define SS_EVENT_LOOP_2P(NAME,TARGET,MASKS,NLMASK) \
template<class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const char* const end=base+len; \
 const EvLoop lp(tl,tab,products,PE,end);PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;ERing* const ring=nullptr;unsigned rpos=0; /* no pending batch, no ring here */ \
 IxBlock blk[2];const char* scan=p; \
 SS_IX_FILL(blk[0],p,NLMASK) \
 for(int cur=0;;cur^=1){ \
  IxBlock& b=blk[cur]; \
  if(b.first>=lim)break; \
  if(b.n==0){line_general<7>(b.first,end,general);break;} /* the last line has no newline */ \
  IxBlock& nx=blk[cur^1];const char* const nf=b.starts[b.n-1]; \
  if(nf<lim){SS_IX_FILL(nx,nf,NLMASK)}else{nx.first=nf;nx.n=0;} \
  const char* q=b.first; \
  for(size_t i=0;i<b.n&&q<lim;++i){const WinMasks w1=MASKS(q);const char* const bb=event_window2(q,w1.c);event_line_v3<true,true>(q,bb,w1,MASKS(bb),b.starts[i]-1,b.pre[i],lp,pend,pp,dmax,send,ring,rpos,general);q=b.starts[i];} \
 } \
 tl.denseMax=max(tl.denseMax,dmax);tl.evEnd=max(tl.evEnd,send); \
}
SS_EVENT_LOOP_2P(events_avx2_2p,SS_V3,win_masks_avx2,nl_mask_avx2)
SS_EVENT_LOOP_2P(events_avx512z_2p,SS_V5,win_masks_avx512z,nl_mask_avx512z)
SS_EVENT_LOOP_2P(events_avx512y_2p,SS_V5,win_masks_avx512y,nl_mask_avx512y)
#undef SS_IX_FILL
// The same for click lines (4 fields, one 64-byte window).
struct ClLoop{ThreadState& tl;KeyTable& tab;const KeyTable& camps;size_t PC;const char* end;DateMemo3* memo;int64_t clickStart;uint64_t wlen;Bucket<CRec>* buckets;KeyTable::Slot* slots;size_t tmask;
 bool dUsers;uint64_t kc,ku,limC;CSlot* cl;uint32_t tid;bool bucket;unsigned ahead,shC;Bucket<DCRec>* dcb; // dense route (dparse)
 ClLoop(ThreadState& tl_,KeyTable& tab_,const KeyTable& camps_,size_t PC_,const char* end_,int64_t clickStart_):tl(tl_),tab(tab_),camps(camps_),PC(PC_),end(end_),memo(date_memo3()),
  clickStart(clickStart_),wlen((uint64_t)(window_end-clickStart_)),buckets(tl_.cb.data()),slots(tab_.slots),tmask(tab_.mask),
  dUsers(dparse.users),kc(dense.kc),ku(dense.ku),limC(dense.limC),cl(denseCl),tid(tl_.tid),bucket(dparse.bucket),ahead(dparse.ahead),shC(dparse.shC),dcb(tl_.dcb.data()){}
};
template<bool DREC,class GEN> SS_V3I const char* click_line_v3(const char* const p,const WinMasks w,const char* const nl,const ClLoop& lp,PendUser* const pend,PendUser*& pp,uint32_t& dmax,uint32_t& send,CRing* const ring,unsigned& rpos,GEN&& general){
 ThreadState& tl=lp.tl;KeyTable& tab=lp.tab;const KeyTable& camps=lp.camps;const size_t PC=lp.PC;
 DateMemo3* const memo=lp.memo;const int64_t clickStart=lp.clickStart;const uint64_t wlen=lp.wlen;
 Bucket<CRec>* const buckets=lp.buckets;KeyTable::Slot* const slots=lp.slots;const size_t tmask=lp.tmask;
 {
  const char* next=nullptr;bool done=false;
  do{
   uint64_t nbit,keep,L; // as in event_line_v3: the dense-record loops know the line end
   if(DREC){L=(uint64_t)(nl-p);nbit=1ull<<(L&63);keep=nbit-1;}
   else{const uint64_t nb=w.n;L=_tzcnt_u64(nb);nbit=_blsi_u64(nb);keep=_blsmsk_u64(nb)>>1;} // L: 64 if there is no newline in the window
   const uint64_t c=w.c&keep,q=w.q&keep;
   const uint64_t st=(c<<1)|1,en=c|nbit;
   uint64_t sq,op;const uint64_t err=quote_errors_v3(q,st,en,sq,op);
   if(__builtin_expect((err|(uint64_t)(_mm_popcnt_u64(c)^3)|(L>>6))!=0,0))break;
   next=DREC?nl+1:p+L+1;done=true; // well-formed, 4 fields
   uint64_t B=st+sq,E=en-(op>>1);
   const unsigned z0=(unsigned)(q&1);
   const char* const s0=p+z0;const unsigned l0=(unsigned)_tzcnt_u64(E)-z0;
   B=_blsr_u64(B);E=_blsr_u64(E);
   const unsigned b1=(unsigned)_tzcnt_u64(B),l1=(unsigned)_tzcnt_u64(E)-b1;
   if(l0==0||l1==0)break;
   // Dense route (DENSE.md): a dense click_id sends a valid click to its slot; ask for the slot's memory right away.
   uint64_t kcn=0;bool dCl=false;uint32_t ucode=0;bool uDense=false;
   if(DREC){
    uint64_t yk,yu;unsigned mk,mu;
    if(__builtin_expect(parse36x2_v3(s0+1,l0-1,p+b1+1,l1-1,yk,yu,mk,mu),1)){
     if(__builtin_expect(s0[0]=='k'&&parse36x2_ok(yk,mk)&&(kcn=dn::index_of(yk,lp.kc))<lp.limC,1)){dCl=true;if(!lp.bucket)__builtin_prefetch(&lp.cl[kcn],1,3);}
     uDense=p[b1]=='u'&&parse36x2_ok(yu,mu)&&dense_user_code(dn::index_of(yu,lp.ku),ucode);
    }else{
     if(s0[0]=='k'&&parse36_v3(s0+1,l0-1,yk)&&(kcn=dn::index_of(yk,lp.kc))<lp.limC){dCl=true;if(!lp.bucket)__builtin_prefetch(&lp.cl[kcn],1,3);}
     uDense=p[b1]=='u'&&parse36_v3(p+b1+1,l1-1,yu)&&dense_user_code(dn::index_of(yu,lp.ku),ucode);
    }
   }
   B=_blsr_u64(B);E=_blsr_u64(E);
   const unsigned b2=(unsigned)_tzcnt_u64(B),l2=(unsigned)_tzcnt_u64(E)-b2;
   B=_blsr_u64(B);E=_blsr_u64(E);
   const unsigned b3=(unsigned)_tzcnt_u64(B),l3=(unsigned)_tzcnt_u64(E)-b3;
   // Copies of a click are identical, so validating before the dedup keeps the same clicks.
   int64_t ts=0;
   uint64_t trel;
   const bool tsOk=ts_v3(p+b3,l3,memo,ts);
   if(!tsOk||(trel=(uint64_t)ts-(uint64_t)clickStart)>=wlen)break; // clickStart <= ts < window_end
   uint64_t k0;uint32_t k1;
   const string_view camp(p+b2,l2);
   const uint32_t ci=__builtin_expect(KeyTable::pack(camp,k0,k1),1)?camps.find_packed(k0,k1):camps.find(camp);if(ci==KeyTable::NONE)break;
   const char* const s1=p+b1;
   // Dense route (DENSE.md): see event_line_v3.
   const uint64_t ckey=(trel<<16)|(uint16_t)ci;
   if(DREC){
    uint32_t user=ucode;
    if(__builtin_expect(uDense,1))dmax=max(dmax,ucode);
    else{ // a general user: through the ID table right here
     PendUser pu;
     if(__builtin_expect(!KeyTable::pack(string_view(s1,l1),pu.k0,pu.k1),0)){done=false;break;} // long user ID
     pu.j=(uint32_t)(KeyTable::hash_packed(pu.k0,pu.k1)&tmask);pu.u=s1;pu.up=&user;
     resolve_users(&pu,&pu+1,tab,tl);
    }
    if(__builtin_expect(dCl,1)){
     if(lp.bucket){
      Bucket<DCRec>& vec=lp.dcb[kcn>>lp.shC];
      if(__builtin_expect(vec.full(),0))vec.reserve(max<size_t>(16,2*vec.capacity()));
      DCRec r;r.key=ckey;r.kc=(uint32_t)kcn;r.user=user;
      vec.append(r);
     }else{
      send=max(send,(uint32_t)kcn+1);
      if(lp.ahead){
       CRing& e=ring[rpos];
       cslot_commit(*e.slot,e.key,e.user,lp.tid);
       e.key=ckey;e.slot=&lp.cl[kcn];e.user=user;
       rpos=rpos+1==lp.ahead?0:rpos+1;
      }else cslot_commit(lp.cl[kcn],ckey,user,lp.tid);
     }
    }
    else{
     CRec r;r.id=s0;r.idlen=l0;r.h=hkey_v3(s0,l0);r.key=ckey;r.user=user;
     Bucket<CRec>& vec=buckets[(size_t)(((r.h>>40)*PC)>>24)];
     if(__builtin_expect(vec.full(),0))vec.reserve(max<size_t>(16,2*vec.capacity()));
     vec.append(r);
    }
   }else{
    bool tableUser=true;
    if(lp.dUsers){uint64_t y;if(s1[0]=='u'&&parse36_v3(s1+1,l1-1,y)&&dense_user_code(dn::index_of(y,lp.ku),ucode)){tableUser=false;dmax=max(dmax,ucode);}}
    if(tableUser&&__builtin_expect(!KeyTable::pack(string_view(s1,l1),k0,k1),0)){done=false;break;} // long user ID
    CRec r;r.id=s0;r.idlen=l0;r.h=hkey_v3(s0,l0);r.key=ckey;r.user=ucode;
    Bucket<CRec>& vec=buckets[(size_t)(((r.h>>40)*PC)>>24)];
    if(__builtin_expect(vec.full(),0)){resolve_users(pend,pp,tab,tl);pp=pend;vec.reserve(max<size_t>(16,2*vec.capacity()));} // grow now: nothing waits on the old storage, and the record appended below stays put
    uint32_t* const up=&vec.e->user;vec.append(r);
    if(tableUser){
     const size_t j=KeyTable::hash_packed(k0,k1)&tmask;
     __builtin_prefetch(&slots[j],0,1);
     PendUser& pu=*pp++;pu.k0=k0;pu.j=(uint32_t)j;pu.u=s1;pu.up=up;pu.k1=k1;
     if(pp==pend+V3_BATCH){resolve_users(pend,pp,tab,tl);pp=pend;}
    }
   }
  }while(0);
  if(!done){
   resolve_users(pend,pp,tab,tl);pp=pend;
   next=line_general<4>(p,lp.end,general);
  }
  return next;
 }
}
#define SS_CLICK_LOOP(NAME,TARGET,MASKS) \
template<bool DREC,class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& camps,size_t PC,int64_t clickStart,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const ClLoop lp(tl,tab,camps,PC,base+len,clickStart);PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;CSlot scratch;CRing ring[RING_MAX];unsigned rpos=0;for(CRing& e:ring){e.key=0;e.slot=&scratch;e.user=0;} /* dmax: largest dense user code seen */ \
 static_assert(!DREC,"the dense-record line code needs the line index"); \
 while(p<lim)p=click_line_v3<DREC>(p,MASKS(p),nullptr,lp,pend,pp,dmax,send,ring,rpos,general); \
 resolve_users(pend,pp,tab,tl);tl.denseMax=max(tl.denseMax,dmax); \
 for(const CRing& e:ring)cslot_commit(*e.slot,e.key,e.user,lp.tid); \
 tl.clEnd=max(tl.clEnd,send); \
}
#define SS_CLICK_LOOP_IX(NAME,TARGET,MASKS,NLMASK) \
template<bool DREC,class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& camps,size_t PC,int64_t clickStart,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const char* const end=base+len; \
 const ClLoop lp(tl,tab,camps,PC,end,clickStart);PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;CSlot scratch;CRing ring[RING_MAX];unsigned rpos=0;for(CRing& e:ring){e.key=0;e.slot=&scratch;e.user=0;} /* dmax: largest dense user code seen */ \
 const char* starts[IX_BLOCK+64];const char* scan=p; \
 while(p<lim){ \
  size_t n=0; \
  for(const char* const stop=scan+IX_BLOCK;scan<stop&&scan<end;scan+=64){uint64_t m=NLMASK(scan);while(m){starts[n++]=scan+_tzcnt_u64(m)+1;m=_blsr_u64(m);}} \
  for(size_t i=0;i<n&&p<lim;++i){click_line_v3<DREC>(p,MASKS(p),starts[i]-1,lp,pend,pp,dmax,send,ring,rpos,general);p=starts[i];} \
  if(scan>=end&&p<lim){if(DREC)p=line_general<4>(p,end,general);else p=click_line_v3<DREC>(p,MASKS(p),nullptr,lp,pend,pp,dmax,send,ring,rpos,general);break;} \
 } \
 resolve_users(pend,pp,tab,tl);tl.denseMax=max(tl.denseMax,dmax); \
 for(const CRing& e:ring)cslot_commit(*e.slot,e.key,e.user,lp.tid); \
 tl.clEnd=max(tl.clEnd,send); \
}
SS_CLICK_LOOP_IX(clicks_avx2_ix,SS_V3,win_masks_avx2,nl_mask_avx2)
SS_CLICK_LOOP_IX(clicks_avx512z_ix,SS_V5,win_masks_avx512z,nl_mask_avx512z)
SS_CLICK_LOOP_IX(clicks_avx512y_ix,SS_V5,win_masks_avx512y,nl_mask_avx512y)
SS_CLICK_LOOP(clicks_avx2,SS_V3,win_masks_avx2)
SS_CLICK_LOOP(clicks_avx512z,SS_V5,win_masks_avx512z)
SS_CLICK_LOOP(clicks_avx512y,SS_V5,win_masks_avx512y)
static void run(const fs::path& data,const fs::path& scratch,const fs::path& output){
 auto __time0=chrono::steady_clock::now();
 // With SS_STATS=1 every TIME line is followed by a CPU line: processor time (all threads, user+system) of that stage.
 const bool __cpuLines=getenv("SS_STATS")!=nullptr;
 auto __cpu_now=[]{timespec t;clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t);return (double)t.tv_sec+1e-9*(double)t.tv_nsec;};
 double __cpu0=__cpu_now();
 auto __mark=[&](const char *label){auto now=chrono::steady_clock::now();cerr<<"TIME "<<label<<" "<<chrono::duration<double>(now-__time0).count()<<"\n";__time0=now;
  if(__cpuLines){const double c=__cpu_now();cerr<<"CPU "<<label<<" "<<c-__cpu0<<"\n";__cpu0=c;}};
 optHuge=!getenv("SS_NOHUGE");optPrefault=getenv("SS_PREFAULT")!=nullptr;optAsyncFree=getenv("SS_ASYNCFREE")!=nullptr;
 auto manifest=read_all(data/"manifest.json");
 if(!parse_ts(json_string(manifest,"window_start"),window_start)||!parse_ts(json_string(manifest,"window_end"),window_end)||window_end<=window_start)throw runtime_error("Bad manifest");
 ingestBase=(int64_t)((uint64_t)window_start-(1ull<<38));
 int nday=(int)((window_end-window_start+DAY-1)/DAY);
 if(nday<=0||nday>1000)throw runtime_error("Unreasonable window length");
 // The scorer may limit CPU by quota rather than by affinity, so trust NCPU when it is set.
 int T=max(1,omp_get_max_threads());
 if(const char* n=getenv("NCPU")){int v=atoi(n);if(v>=1&&v<=1024)T=v;}
 omp_set_num_threads(T);
 // Map the big inputs up front so the kernel starts reading them while links are processed.
 vector<Chunk> chunks;uint64_t eventBytes=0,clickBytes=0;
 static constexpr size_t CH=size_t(2)<<20;
 const char* sampleBase=nullptr;size_t sampleLen=0;
 FileGroup eventFiles,clickFiles,linkFiles;
 auto add_files=[&](const fs::path& dir,bool click,uint64_t& bytes,FileGroup& group){
  for(const Mapped& m:map_files(csv_files(dir),group)){if(!m.len)continue;bytes+=m.len;
   if(!click&&m.len>sampleLen){sampleLen=m.len;sampleBase=m.base;}
   for(size_t from=0;from<m.len;from+=CH)chunks.push_back({m.base,m.len,from,from+CH,click});}
 };
 add_files(data/"events",false,eventBytes,eventFiles);
 add_files(data/"clicks",true,clickBytes,clickFiles);
 // Lines per byte, measured on the start of the largest event file, sizes the bucket buffers tightly.
 uint64_t estEvents=eventBytes/70;
 if(sampleLen>4096){const size_t n=min<size_t>(sampleLen,size_t(4)<<20);const uint64_t nl=(uint64_t)count_newlines(sampleBase,n);
  if(nl>16)estEvents=(uint64_t)((double)eventBytes*(double)nl/(double)n)+1;}
 const uint64_t estClicks=clickBytes/36;
 __mark("MAP");
 // ---- Dense IDs: find the generator's keys from the first lines of one event file and one click file. ----
 // (SS_NODENSE=1 turns the dense route off.)
 if(!getenv("SS_NODENSE")&&sampleLen){
  // 24 IDs per column: from the first lines of a small file, six each from four places of a larger one
  // (so that a few odd lines at the head of a file do not decide for the whole file).
  auto sample=[](const char* base,size_t len,int col,char letter){
   if(len<=(size_t(1)<<16))return dn::sample_ids(base,len,col,letter,24);
   vector<uint64_t> out;
   for(size_t k=0;k<4;++k){const size_t off=len/4*k; // a place inside the file starts in the middle of a line: sample_ids skips to the next one
    for(uint64_t y:dn::sample_ids(base+off,min<size_t>(len-off,size_t(1)<<14),col,letter,6))out.push_back(y);}
   return out;
  };
  const vector<uint64_t> es=sample(sampleBase,sampleLen,0,'e'),us=sample(sampleBase,sampleLen,1,'u');
  vector<uint64_t> cs;for(const Chunk& c:chunks)if(c.click&&c.from==0){cs=sample(c.base,c.len,0,'k');if(cs.size()>=12)break;}
  dn::Info d;
  d.limE=min<uint64_t>(estEvents+estEvents/8+4096,uint64_t(1)<<31);
  d.limC=min<uint64_t>(estClicks+estClicks/8+4096,uint64_t(1)<<31);
  d.limI=min<uint64_t>(estEvents+estClicks+4096,uint64_t(1)<<27);
  const bool users=dn::infer(us,d.limI<<dn::GEN_ALIAS_BITS,uint64_t(1)<<dn::GEN_ALIAS_BITS,d.ku,T);
  d.events=users&&dn::infer(es,d.limE,1,d.ke,T);
  d.clicks=users&&dn::infer(cs,d.limC,1,d.kc,T);
  dense=d;
  // Tests shrink the limits to put IDs just at and over them.
  if(const char* v=getenv("SS_TEST_LIME"))dense.limE=min<uint64_t>(dense.limE,strtoull(v,nullptr,10));
  if(const char* v=getenv("SS_TEST_LIMC"))dense.limC=min<uint64_t>(dense.limC,strtoull(v,nullptr,10));
  if(const char* v=getenv("SS_TEST_LIMI"))dense.limI=min<uint64_t>(dense.limI,strtoull(v,nullptr,10));
 }
 if(dense.events||dense.clicks)denseComplex=(uint64_t*)map_huge(((dense.limI+63)/64+1)*8);
 // Test only: marks every n-th user index complex before anything else does, so that both user routes occur for dense-looking IDs.
 if(const char* v=getenv("SS_TEST_COMPLEX"))if(denseComplex){const uint64_t n=max<uint64_t>(1,strtoull(v,nullptr,10));for(uint64_t i=0;i<dense.limI;i+=n)denseComplex[i>>6]|=1ull<<(i&63);}
 // Parse-stage switches of the dense route:
 //   SS_DENSE_USERS=0    dense user IDs go through the ID table like all others, links included (default: they get
 //                       DENSE_USER codes straight from the text if the links stage allows it; see denseUsers)
 //   SS_DENSE_RECORDS    what happens to event / click lines with a dense ID (needs dense users):
 //                       slot (default)  the parse stage writes them straight into the slot arrays denseEv / denseCl
 //                       bucket          per-thread buckets by counter range (DRec / DCRec); a later pass fills the slots
 //                       0               the general buckets, as for every other ID
 //   SS_DENSE_AHEAD=n    slot form: write a slot n dense rows after asking for its memory (0-16, default 8; 0 = at the end of
 //                       the line). Measured on the scoring machine at 20M events: parse stage 525 ms at 0, 355 at 2, 341 at 8.
 //   SS_DSHIFT, SS_CSHIFT  bucket form: log2 of the counters per event / click bucket (4-16 / 4-24, default 14)
 //   SS_DENSE_DUMP=dir   test only: writes every record of the parse stage as text and stops after the parse stage ("-": only stops)
 {const char* rec=getenv("SS_DENSE_RECORDS");const bool recOn=!(rec&&rec[0]=='0');
  dparse.events=dense.events&&recOn;dparse.clicks=dense.clicks&&recOn;
  dparse.bucket=rec&&string_view(rec)=="bucket";}
 if(const char* v=getenv("SS_DENSE_AHEAD")){const int x=atoi(v);if(x>=0&&x<=(int)RING_MAX)dparse.ahead=(unsigned)x;}
 if(const char* v=getenv("SS_DSHIFT")){const int x=atoi(v);if(x>=4&&x<=16)dparse.shE=(unsigned)x;}
 if(const char* v=getenv("SS_CSHIFT")){const int x=atoi(v);if(x>=4&&x<=24)dparse.shC=(unsigned)x;}
 const char* const dumpDir=getenv("SS_DENSE_DUMP");
 const bool dumpText=dumpDir&&strcmp(dumpDir,"-"); // "-": write nothing, only stop after the parse stage (for measuring it)
 cerr<<"dense: events "<<dense.events<<" clicks "<<dense.clicks<<"\n";
 if(getenv("SS_STATS"))cerr<<"STATS dense keys "<<dense.ke<<" "<<dense.ku<<" "<<dense.kc<<" limits "<<dense.limE<<" "<<dense.limC<<" "<<dense.limI<<"\n";
 __mark("KEYS");
 // ---- Linked IDs: parse link files in parallel, intern both IDs lock-free, union with compare-and-swap. ----
 vector<Chunk> linkChunks;uint64_t linkLines=0,linkBytes=0,linkFileCount=0;
 for(const Mapped& m:map_files(csv_files(data/"links"),linkFiles)){if(!m.len)continue;
  ++linkLines; // a last line without '\n'
  linkBytes+=m.len;++linkFileCount;
  for(size_t from=0;from<m.len;from+=CH)linkChunks.push_back({m.base,m.len,from,from+CH,false});}
 static const bool idBlocks=!getenv("SS_NOIDBLOCKS"); // measuring switch: per-thread ID blocks in the links stage
 // Room for IDs that only appear in events and clicks: about 3x what the generator produces.
 // If a dataset has more, the extra ones take the thread-private fallback path (slower, still exact).
 uint64_t maxNew64=(estEvents+estClicks)/28+4096;
 if(const char* v=getenv("SS_TEST_MAXNEW"))maxNew64=strtoull(v,nullptr,10); // tests force the fallback path with a tiny value
 // Dense users are decided before any line is read. Table numbers must stay below DENSE_USER; a link
 // line has at least four bytes, which bounds the IDs the files can name without counting lines.
 // (SS_DENSE_USERS=0 turns the route off as a whole: the links stage and the parse stage have to agree.)
 const bool denseLinks=dense.events||dense.clicks;
 {const char* v=getenv("SS_DENSE_USERS");
  const uint64_t idBound=2*(linkBytes/4+linkFileCount)+(uint64_t)T*KeyTable::ID_BLOCK+maxNew64;
  denseUsers=denseLinks&&idBound<DENSE_USER&&(dense.limI<<dn::ALIAS_BITS)<=DENSE_USER&&!(v&&v[0]=='0');}
 if(denseUsers)denseComp=(atomic<uint32_t>*)map_huge((dense.limI+1)*sizeof(uint32_t));
 dparse.users=denseUsers;
 if(!denseUsers)dparse.events=dparse.clicks=false; // the record loops decode both IDs of a line together
 if(!dparse.events&&!dparse.clicks)dparse.bucket=false;
 // SS_DENSE_USERS=code: the line loops decode the user ID of every dense-record event line themselves (default:
 // raw, the text goes into the slot and is decoded once per run of equal users after parsing; slot form only).
 {const char* v=getenv("SS_DENSE_USERS");dparse.raw=dparse.events&&!dparse.bucket&&!(v&&string_view(v)=="code");}
 // Event lines with raw users use the two-phase loop (SS_EVENT_LOOP_2P) unless SS_DENSE_AHEAD is given: then the
 // loop with the ring of that length (0 = write at the end of the line) runs, for comparison.
 dparse.twoPhase=dparse.raw&&!getenv("SS_DENSE_AHEAD");
 cerr<<"dense parse: users "<<dparse.users<<" event records "<<dparse.events<<" click records "<<dparse.clicks<<" bucket "<<dparse.bucket<<" raw users "<<dparse.raw<<" two-phase "<<dparse.twoPhase<<"\n";
 // First pass over the link files.
 // Without dense users it only counts lines (the table and the union-find arrays are sized by them).
 // With dense users it is the only pass that reads the files: a "simple" line - two dense user IDs
 // with aliases 0-7 of one user index - is applied to that index's alias word at once; every other
 // line marks the dense indexes it names complex and is kept (its two fields) for the ID table.
 // A complex index keeps the general route for all its IDs, so the simple lines already applied to its
 // word have to reach the table too: they are replayed from the word afterwards (same groups).
 struct LinkGen{const char* a;const char* b;uint32_t la,lb;};
 struct alignas(64) LinkOut{vector<LinkGen> gen;vector<uint32_t> cplx;uint32_t maxIdx=0;};
 vector<LinkOut> lout(denseUsers?T:0);
 {
  bool v3=__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("popcnt")&&__builtin_cpu_supports("lzcnt");
  if(const char* isa=getenv("SS_ISA"))if(string_view(isa)=="sse2")v3=false;
  void (*const link_dense)(const char*,size_t,const char*,size_t,LinkDense&)=v3?link_dense_v3:link_dense_base;
  #pragma omp parallel for num_threads(T) schedule(dynamic,1) reduction(+:linkLines)
  for(size_t c=0;c<linkChunks.size();++c){const Chunk& ck=linkChunks[c];
   if(!denseUsers){linkLines+=count_newlines(ck.base+ck.from,min(ck.to,ck.len)-ck.from);continue;}
   if(ck.from>=ck.len)continue;
   const char* const end=ck.base+ck.len;
   const char* nl=(const char*)memchr(ck.from?ck.base+ck.from-1:ck.base,'\n',ck.from?ck.len-(ck.from-1):ck.len);
   if(!nl)continue;
   const char* const lim=ck.base+min(ck.to,ck.len);
   LinkOut& out=lout[omp_get_thread_num()];
   // Simple lines wait in a small batch so that their index's word is in the cache when it is changed.
   static constexpr int SB=8;
   struct Simple{uint32_t idx,aa,ab;} sb[SB];int ns=0;uint32_t maxIdx=out.maxIdx;
   const auto apply=[&]{for(int i=0;i<ns;++i)comp_union(sb[i].idx,sb[i].aa,sb[i].ab);ns=0;};
   for(const char* p=nl+1;p<lim;){
    string_view f[2];const char* next;
    const bool ok=link_split(p,end,f,next);
    p=next;
    if(!ok)continue;
    LinkDense r;link_dense(f[0].data(),f[0].size(),f[1].data(),f[1].size(),r);
    if(r.da&&r.db&&r.ia==r.ib&&r.aa<8&&r.ab<8){
     maxIdx=max(maxIdx,(uint32_t)r.ia);
     if(r.aa!=r.ab){__builtin_prefetch(&denseComp[r.ia],1,1);sb[ns++]={(uint32_t)r.ia,r.aa,r.ab};if(ns==SB)apply();}
     continue;
    }
    // Whoever turns an index's bit on notes the index for the replay (so each index is noted once).
    if(r.da&&!dense_set_complex(r.ia))out.cplx.push_back((uint32_t)r.ia);
    if(r.db&&!dense_set_complex(r.ib))out.cplx.push_back((uint32_t)r.ib);
    out.gen.push_back({f[0].data(),f[1].data(),(uint32_t)f[0].size(),(uint32_t)f[1].size()});
   }
   apply();out.maxIdx=maxIdx;
  }
 }
 // The pairs to replay: for every complex index, each alias that the simple lines put into a group
 // with a smaller one. (Indexes the test switch marked complex were never noted; they are looked up here.)
 struct Replay{uint32_t idx;uint8_t a,b;};
 vector<Replay> replay;uint64_t genLines=0;
 if(denseUsers){
  const auto add_index=[&](uint32_t idx){const uint32_t w=denseComp[idx].load(memory_order_relaxed)^COMP_IDENT;
   for(uint32_t a=1;a<8;++a){const uint32_t r=comp_root(w,a);if(r!=a)replay.push_back({idx,(uint8_t)a,(uint8_t)r});}};
  uint32_t maxIdx=0;
  for(const LinkOut& o:lout){genLines+=o.gen.size();maxIdx=max(maxIdx,o.maxIdx);for(uint32_t idx:o.cplx)add_index(idx);}
  if(const char* v=getenv("SS_TEST_COMPLEX")){const uint64_t n=max<uint64_t>(1,strtoull(v,nullptr,10));
   for(uint64_t i=0;i<=maxIdx&&i<dense.limI;i+=n)add_index((uint32_t)i);}
  linkLines=genLines+replay.size(); // the lines that go through the table
 }
 // Every such line names at most two IDs; with ID blocks each thread may leave part of its last block unused.
 const uint64_t maxLinked=2*linkLines+(idBlocks?(uint64_t)T*KeyTable::ID_BLOCK:0);
 __mark("LINKMAP");
 if(maxLinked+maxNew64>=0x60000000u)throw runtime_error("Too many IDs");
 const uint32_t maxIds=(uint32_t)(maxLinked+maxNew64);
 // Table size. Link files usually repeat IDs, so the table is sized for three quarters of the IDs the
 // link lines could name plus all allowed new ones, at 62% full. After the links stage the limit for
 // new IDs is set from the linked IDs actually seen; even if every link ID was different the table then
 // ends at most 80% full. (SS_TABLE_FULL=1: the previous, roomier size.)
 const uint64_t likelyIds=max<uint64_t>(maxLinked,maxLinked*3/4+maxNew64);
 KeyTable tab(getenv("SS_TABLE_FULL")?(size_t)maxIds*2:(size_t)(likelyIds*8/5));
 tab.begin_parallel(0,maxIds);
 // parent[x] <= x always (a root links to a smaller root), so the forest can never form a cycle.
 // (Initialised by a pass below. Mapping the two arrays as zero pages with "value XOR index" entries,
 // to skip that pass, measured slower: every page then takes its faults in the middle of the hot loops.)
 static_assert(sizeof(atomic<uint32_t>)==sizeof(uint32_t));
 atomic<uint32_t>* const parent=(atomic<uint32_t>*)map_huge((maxLinked+1)*sizeof(uint32_t)); // stays mapped: the process ends with _exit
 auto find_root=[&](uint32_t u)->uint32_t{
  for(;;){
   uint32_t p=parent[u].load(memory_order_relaxed);if(p==u)return u;
   uint32_t gp=parent[p].load(memory_order_relaxed);if(gp==p)return p;
   parent[u].compare_exchange_weak(p,gp); // path halving; losing the race is harmless
   u=gp;
  }
 };
 // best[r]: the ID with the byte-wise smallest name seen so far in root r's group.
 atomic<uint32_t>* const best=(atomic<uint32_t>*)map_huge((maxLinked+1)*sizeof(uint32_t));
 atomic<size_t> nextLink{0},nextReplay{0};uint64_t idHoles=0;
 size_t maxGenList=0;for(const LinkOut& o:lout)maxGenList=max(maxGenList,o.gen.size());
 __mark("LINKSETUP");
 #pragma omp parallel num_threads(T) reduction(+:idHoles)
 {
  #pragma omp for schedule(static)
  for(uint64_t i=0;i<maxLinked;++i){parent[i].store((uint32_t)i,memory_order_relaxed);best[i].store((uint32_t)i,memory_order_relaxed);}
  // Lines are handled in small batches so the table slots of a batch, and then the parent entries of
  // its IDs, are on their way into the cache before they are needed.
  static constexpr int LB=16;
  struct LinkPend{string_view f[2];uint64_t k0[2];uint32_t k1[2];uint32_t j[2];char txt[2][16];}; // k1 == 0: key too long to pack; txt: the text of a replayed ID
  LinkPend pend[LB];uint32_t ida[LB],idb[LB];
  const size_t tmask=tab.mask;KeyTable::IdBlock blk;
  auto id_of=[&](const LinkPend& q,int k)->uint32_t{ // cannot be full: the table holds every ID the link lines can name
   if(!q.k1[k])return tab.find_or_add_long(q.f[k]);
   return idBlocks?tab.find_or_add_packed_block(q.k0[k],q.k1[k],q.j[k],blk):tab.find_or_add_packed(q.k0[k],q.k1[k],q.j[k],q.f[k]);
  };
  auto resolve=[&](int nb){
   for(int i=0;i<nb;++i){const LinkPend& q=pend[i];
    const uint32_t a=id_of(q,0),b=id_of(q,1);
    ida[i]=a;idb[i]=b;__builtin_prefetch(&parent[a],1,1);__builtin_prefetch(&parent[b],1,1);
   }
   // One more level of each chain, so the walks below mostly find their entries in the cache.
   for(int i=0;i<nb;++i){
    __builtin_prefetch(&parent[parent[ida[i]].load(memory_order_relaxed)],1,1);
    __builtin_prefetch(&parent[parent[idb[i]].load(memory_order_relaxed)],1,1);
   }
   for(int i=0;i<nb;++i){
    uint32_t a=ida[i],b=idb[i];
    for(;;){
     a=find_root(a);b=find_root(b);if(a==b)break;
     if(a<b)swap(a,b);
     uint32_t expect=a;if(parent[a].compare_exchange_strong(expect,b))break;
    }
   }
  };
  const auto push_pair=[&](int& nb){ // pend[nb].f[0..1] are set: hash them, ask for their table slots, resolve full batches
   LinkPend& q=pend[nb];
   for(int k=0;k<2;++k){
    if(KeyTable::pack(q.f[k],q.k0[k],q.k1[k])){q.j[k]=(uint32_t)(KeyTable::hash_packed(q.k0[k],q.k1[k])&tmask);__builtin_prefetch(&tab.slots[q.j[k]],1,1);}
    else q.k1[k]=0;
   }
   if(++nb==LB){resolve(nb);nb=0;}
  };
  if(!denseUsers)for(;;){
   size_t c=nextLink.fetch_add(1,memory_order_relaxed);if(c>=linkChunks.size())break;
   const Chunk& ck=linkChunks[c];
   // Every data line that begins in [from,to), as in for_lines; exactly two non-empty fields or the line is skipped.
   if(ck.from>=ck.len)continue;
   const char* const end=ck.base+ck.len;
   const char* nl=(const char*)memchr(ck.from?ck.base+ck.from-1:ck.base,'\n',ck.from?ck.len-(ck.from-1):ck.len);
   if(!nl)continue;
   const char* const lim=ck.base+min(ck.to,ck.len);
   int nb=0;
   for(const char* p=nl+1;p<lim;){
    const char* next;
    if(link_split(p,end,pend[nb].f,next))push_pair(nb);
    p=next;
   }
   resolve(nb);
  }
  else{
   // With dense users: the lines the first pass kept, a piece of one thread's list at a time, then the replayed pairs.
   static constexpr size_t PIECE=2048;
   int nb=0;
   for(;;){
    const size_t w=nextLink.fetch_add(1,memory_order_relaxed);
    const size_t t=w%(size_t)T,from=(w/(size_t)T)*PIECE; // piece `from` of thread t's list
    if(from>=maxGenList)break;
    const vector<LinkGen>& g=lout[t].gen;
    for(size_t i=from,e=min(g.size(),from+PIECE);i<e;++i){
     pend[nb].f[0]=string_view(g[i].a,g[i].la);pend[nb].f[1]=string_view(g[i].b,g[i].lb);push_pair(nb);
    }
   }
   for(;;){
    const size_t from=nextReplay.fetch_add(PIECE,memory_order_relaxed);if(from>=replay.size())break;
    for(size_t i=from,e=min(replay.size(),from+PIECE);i<e;++i){
     const Replay& rp=replay[i];LinkPend& q=pend[nb];
     for(int k=0;k<2;++k){const size_t n=text36_fast('u',dn::value_of(((uint64_t)rp.idx<<dn::GEN_ALIAS_BITS)|(k?rp.b:rp.a),dense.ku),q.txt[k]);q.f[k]=string_view(q.txt[k],n);}
     push_pair(nb);
    }
   }
   resolve(nb);
  }
  idHoles+=blk.end-blk.next;
 }
 // nlinked: one past the highest linked ID number (with ID blocks a few numbers below it were never used:
 // they stay empty one-member groups that nothing refers to).
 const uint32_t nlinked=min(tab.nextId.load(),tab.idLimit);
 // Room for maxNew64 more IDs from events and clicks (also what SS_TEST_MAXNEW limits).
 tab.idLimit=(uint32_t)min<uint64_t>(tab.idLimit,(uint64_t)nlinked+maxNew64);
 __mark("LINKREAD");
 // Flatten: every linked ID that is not its group's root offers its name to the root (the byte-wise
 // smallest wins) and has its table entry pointed at the root, which is the person number.
 {
  PName* names=tab.newNames;
  #pragma omp parallel num_threads(T)
  {
   // In small batches and three steps, so that what a step needs (the root's best entry, the best
   // name so far, the table slot to rewrite) was asked for by the step before.
   static constexpr uint32_t FB=16;
   struct Fix{uint64_t k0;size_t j;uint32_t k1,r,i;bool packed;};
   const size_t tmask=tab.mask;
   #pragma omp for schedule(static)
   for(uint32_t b0=0;b0<nlinked;b0+=FB){
    Fix fix[FB];int nf=0;const uint32_t b1=nlinked-b0<FB?nlinked:b0+FB;
    for(uint32_t i=b0;i<b1;++i){
     const uint32_t r=find_root(i);if(r==i)continue;
     Fix& f=fix[nf++];f.i=i;f.r=r;__builtin_prefetch(&best[r],1,1);
     f.packed=KeyTable::pack(names[i].view(),f.k0,f.k1); // longer keys are in tab.longs, below
     if(f.packed){f.j=KeyTable::hash_packed(f.k0,f.k1)&tmask;__builtin_prefetch(&tab.slots[f.j],1,1);}
    }
    for(int q=0;q<nf;++q)__builtin_prefetch(&names[best[fix[q].r].load(memory_order_relaxed)],0,1);
    for(int q=0;q<nf;++q){const Fix& f=fix[q];
     uint32_t cur=best[f.r].load(memory_order_relaxed);
     while(name_less(names[f.i],names[cur])&&!best[f.r].compare_exchange_weak(cur,f.i)){}
     if(f.packed)tab.set_packed(f.k0,f.k1,f.j,f.r);
    }
   }
   // A root's own name is no longer needed once every member has been compared, and no other person reads it.
   #pragma omp for schedule(static)
   for(uint32_t i=0;i<nlinked;++i)if(parent[i].load(memory_order_relaxed)==i){const uint32_t bi=best[i].load(memory_order_relaxed);if(bi!=i)names[i]=names[bi];}
  }
  for(auto& l:tab.longs)if(l.v<nlinked)l.v=find_root(l.v);
 }
 __mark("FLATTEN");
 __mark("LINKS"); cerr<<"linked IDs: "<<nlinked-idHoles<<"\n";
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
 // SS_ISA=avx512z / avx512y pick an AVX-512 mask builder (see the line loops) when the CPU has AVX-512BW+VL;
 // without it they fall back to avx2. The default is avx2.
 const bool hasV3=__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("popcnt")&&__builtin_cpu_supports("lzcnt");
 const bool hasV5=hasV3&&__builtin_cpu_supports("avx512f")&&__builtin_cpu_supports("avx512bw")&&__builtin_cpu_supports("avx512vl");
 enum{ISA_SSE2,ISA_AVX2,ISA_AVX512Z,ISA_AVX512Y};
 int isaUse=hasV3?ISA_AVX2:ISA_SSE2;
 if(const char* isa=getenv("SS_ISA")){const string_view w(isa);
  if(w=="sse2")isaUse=ISA_SSE2;
  else if(w=="avx512z"&&hasV5)isaUse=ISA_AVX512Z;
  else if(w=="avx512y"&&hasV5)isaUse=ISA_AVX512Y;}
 // The vector loops locate line starts ahead of cutting the lines (see SS_EVENT_LOOP_IX): measured 5% faster
 // in the parse stage on the scoring machine. SS_LINES=plain switches back to the loop without the index.
 const bool lineIndex=isaUse!=ISA_SSE2&&!(getenv("SS_LINES")&&string_view(getenv("SS_LINES"))=="plain");
 cerr<<"splitter: "<<(isaUse==ISA_AVX512Z?"avx512z":isaUse==ISA_AVX512Y?"avx512y":isaUse==ISA_AVX2?"avx2":"sse2")<<(lineIndex?"+index":"")
  <<"\n";
 vector<ThreadState> tls(T);
 // Tests: SS_TEST_SIDE=1 makes the general handler keep every ingest_ts on the side list (use with
 // SS_ISA=sse2 so every line takes that handler); SS_TEST_TAG0=1 makes the event dedup compare the
 // ID text on every occupied slot it probes instead of only on a tag match.
 const bool testSide=getenv("SS_TEST_SIDE")!=nullptr;
 const uint32_t tagKeep=getenv("SS_TEST_TAG0")?0u:~0u;
 atomic<size_t> nextChunk{0};
 const int64_t clickStart=window_start-WEEK;
 // Dense slots: zero pages, touched by whoever writes a slot first.
 if(dparse.events&&!dparse.bucket)denseEv=(ESlot*)map_huge((size_t)(dense.limE+1)*sizeof(ESlot));
 if(dparse.clicks&&!dparse.bucket)denseCl=(CSlot*)map_huge((size_t)(dense.limC+1)*sizeof(CSlot));
 // Bucket form: one bucket per 2^sh counters and thread.
 const size_t DE=dparse.bucket&&dparse.events?(size_t)(dense.limE>>dparse.shE)+1:0,DC=dparse.bucket&&dparse.clicks?(size_t)(dense.limC>>dparse.shC)+1:0;
 // How full they get: counters are 0 .. D-1 with D = number of distinct IDs, which is smaller than the number of
 // lines (copies) and well below the limit. The generator shuffles the lines, so the largest counter in the first
 // lines of a file gives D closely; lines per bucket = lines * 2^sh / D. A wrong guess only costs growth.
 auto dense_span=[&](const char* base,size_t len,char letter,uint64_t key,uint64_t limit)->uint64_t{
  if(!base)return limit;
  uint64_t n=0,m=0;
  for(uint64_t y:dn::sample_ids(base,min<size_t>(len,size_t(256)<<10),0,letter,SIZE_MAX)){const uint64_t c=dn::index_of(y,key);if(c<limit){++n;m=max(m,c);}}
  return n<256?limit:min(limit,m+1+4*(m+1)/n);
 };
 const char* clickBase=nullptr;size_t clickLen=0;
 for(const Chunk& c:chunks)if(c.click&&c.from==0&&c.len>clickLen){clickBase=c.base;clickLen=c.len;}
 const uint64_t spanE=DE?max<uint64_t>(1,dense_span(sampleBase,sampleLen,'e',dense.ke,dense.limE)):1;
 const uint64_t spanC=DC?max<uint64_t>(1,dense_span(clickBase,clickLen,'k',dense.kc,dense.limC)):1;
 // Records per bucket and thread: the expected number, 3% for the line estimate, five standard deviations.
 auto dense_reserve=[&](uint64_t lines,uint64_t span,unsigned sh)->size_t{
  const double lam=(double)lines*(double)(uint64_t(1)<<sh)/((double)span*(double)T);
  return (size_t)(lam*1.03+5.0*sqrt(lam))+16;
 };
 const size_t rdFull=DE?dense_reserve(estEvents,spanE,dparse.shE):0,rdcFull=DC?dense_reserve(estClicks,spanC,dparse.shC):0;
 const size_t usedE=DE?min<size_t>(DE,(size_t)((spanE-1)>>dparse.shE)+1):0,usedC=DC?min<size_t>(DC,(size_t)((spanC-1)>>dparse.shC)+1):0;
 if(getenv("SS_STATS")&&(DE||DC))cerr<<"STATS dense buckets events "<<DE<<" (span "<<spanE<<", "<<usedE<<" x "<<rdFull<<" records) clicks "<<DC<<" (span "<<spanC<<", "<<usedC<<" x "<<rdcFull<<" records)\n";
 if(T>65535)throw runtime_error("Too many threads"); // ESlot::thread
 #pragma omp parallel num_threads(T)
 {
  ThreadState& tl=tls[omp_get_thread_num()];
  tl.eb.resize(PE);tl.cb.resize(PC);tl.uev.resize(UP);tl.ucl.resize(UP);
  tl.tid=(uint32_t)omp_get_thread_num();
  {// With the dense slots on, the hash buckets only get the few general rows: start them small (they grow).
   const size_t re=dparse.events?(size_t)(estEvents/((uint64_t)T*PE)/64)+16:(size_t)(estEvents/((uint64_t)T*PE)*9/8)+16;
   const size_t rc=dparse.clicks?(size_t)(estClicks/((uint64_t)T*PC)/64)+16:(size_t)(estClicks/((uint64_t)T*PC)*9/8)+16;
   // Dense buckets past the counters in use stay small.
   const size_t need=PE*(re*sizeof(PRec)+64)+PC*(rc*sizeof(CRec)+64)
    +usedE*(rdFull*sizeof(DRec)+64)+(DE-usedE)*(16*sizeof(DRec)+64)+usedC*(rdcFull*sizeof(DCRec)+64)+(DC-usedC)*(16*sizeof(DCRec)+64);
   tlArena.stage=0;tlArena.add_region(need+(size_t(4)<<20),need); // one pre-faulted block per thread for its buckets
   for(auto& v:tl.eb)v.reserve(re);
   for(auto& v:tl.cb)v.reserve(rc);
   tl.db.resize(DE);tl.dcb.resize(DC);
   for(size_t i=0;i<DE;++i)tl.db[i].reserve(i<usedE?rdFull:16);
   for(size_t i=0;i<DC;++i)tl.dcb[i].reserve(i<usedC?rdcFull:16);
  }
  // SS_DENSE_DUMP: text forms for the test dump (IDs can hold any byte, so they are written in hex).
  auto dump_user=[&](uint32_t user)->string{ // what a record's user field stands for
   if(user&LOCAL_USER)return "L:"+to_string(user&~LOCAL_USER);
   if(user&DENSE_USER){char t[9];const uint64_t c=user&~DENSE_USER;
    const size_t n=dn::text36('u',dn::value_of(((c>>dn::ALIAS_BITS)<<dn::GEN_ALIAS_BITS)|(c&((1u<<dn::ALIAS_BITS)-1)),dense.ku),t);return "U:"+hexstr(string_view(t,n));}
   return "T:"+to_string(user);
  };
  auto user_of=[&](string_view u)->uint32_t{
   if(dparse.users){uint32_t code;if(dense_user(u.data(),u.size(),code)){tl.denseMax=max(tl.denseMax,code);return code;}}
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
   const uint64_t h=hkey(f[0]);uint64_t k=0;unsigned cat=0;uint32_t user=0;
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
      k=((uint64_t)(ts-window_start+1)<<EV_TS)|(purchase?EV_PURCHASE:0)|(uint64_t)(uint32_t)price;user=user_of(f[1]);
      if(purchase){const uint32_t c=products.find(f[5]);cat=c==KeyTable::NONE?0:(unsigned)(uint16_t)c;}
     }
    }
   }
   // The route depends on the event_id text only (DENSE.md), so every copy of an event takes the same one.
   uint64_t ve=0;
   const bool dEv=dparse.events&&dense_counter(f[0].data(),f[0].size(),'e',dense.ke,dense.limE,ve);
   if(dumpText){tl.dumpRows+=string("E\t")+(dEv?"D":"P")+"\t"+hexstr(f[0])+"\t"+to_string(ingest)+"\t"+to_string(k)+"\t"+to_string(cat)+"\t"+(k?hexstr(f[1])+"\t"+dump_user(user):string("-\t-"))+"\n";}
   if(dEv){
    if(dparse.bucket){DRec r;r.k=k;r.ingest=ingest;r.user=user;r.cat=(uint16_t)cat;r.lo=(uint16_t)((uint32_t)ve&((1u<<dparse.shE)-1));tl.db[(size_t)(ve>>dparse.shE)].push_back(r);}
    else{tl.evEnd=max(tl.evEnd,(uint32_t)ve+1);eslot_commit(denseEv[ve],k?k:ESLOT_INVALID,eslot_uct(user,cat,tl.tid),ingest);}
    return;
   }
   uint64_t code=testSide?INGEST_SIDE:ingest_code(ingest);
   if(code==INGEST_SIDE)tl.ingestSide.emplace_back(f[0].data(),ingest); // rare: keep the exact value on the side
   PRec r=make_prec(f[0].data(),h,code,k,cat);r.user=user;
   tl.eb[(size_t)(((h>>40)*PE)>>24)].push_back(r);
  };
  auto on_click=[&](const string_view* f){
   if(f[0].empty())return;
   // Copies of a click are identical, so validating before the dedup keeps the same clicks.
   int64_t ts=0;
   if(f[1].empty()||!parse_ts_fast(f[3],ts)||ts<clickStart||ts>=window_end)return;
   const uint32_t ci=camps.find(f[2]);if(ci==KeyTable::NONE)return;
   const uint64_t ckey=((uint64_t)(ts-clickStart)<<16)|(uint16_t)ci;const uint32_t user=user_of(f[1]);
   uint64_t kcn=0;
   const bool dCl=dparse.clicks&&dense_counter(f[0].data(),f[0].size(),'k',dense.kc,dense.limC,kcn);
   if(dumpText){tl.dumpRows+=string("C\t")+(dCl?"D":"P")+"\t"+hexstr(f[0])+"\t"+to_string(ckey)+"\t"+hexstr(f[1])+"\t"+dump_user(user)+"\n";}
   if(dCl){
    if(dparse.bucket){DCRec r;r.key=ckey;r.kc=(uint32_t)kcn;r.user=user;tl.dcb[(size_t)(kcn>>dparse.shC)].push_back(r);}
    else{tl.clEnd=max(tl.clEnd,(uint32_t)kcn+1);cslot_commit(denseCl[kcn],ckey,user,tl.tid);}
    return;
   }
   CRec r;r.id=f[0].data();r.idlen=(uint32_t)f[0].size();r.h=hkey(f[0]);r.key=ckey;r.user=user;
   tl.cb[(size_t)(((r.h>>40)*PC)>>24)].push_back(r);
  };
  for(;;){
   size_t c=nextChunk.fetch_add(1,memory_order_relaxed);if(c>=chunks.size())break;
   const Chunk& ck=chunks[c];
   // Dense records (DREC loops) exist with the line index only; SS_LINES=plain then applies to the other kind of file.
   const bool drec=ck.click?dparse.clicks:dparse.events;
   #define SS_RUN_LOOPS(ISA) \
    if(ck.click){if(drec)clicks_##ISA##_ix<true>(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click); \
     else if(lineIndex)clicks_##ISA##_ix<false>(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click); \
     else clicks_##ISA<false>(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click);} \
    else{if(drec&&dparse.twoPhase)events_##ISA##_2p(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); \
     else if(drec)events_##ISA##_ix<true>(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); \
     else if(lineIndex)events_##ISA##_ix<false>(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); \
     else events_##ISA<false>(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event);}
   if(isaUse==ISA_AVX2){SS_RUN_LOOPS(avx2)}
   else if(isaUse==ISA_AVX512Z){SS_RUN_LOOPS(avx512z)}
   else if(isaUse==ISA_AVX512Y){SS_RUN_LOOPS(avx512y)}
   #undef SS_RUN_LOOPS
   else{
    if(ck.click)for_records<4>(ck.base,ck.len,ck.from,ck.to,warm_user,on_click);
    else for_records<7>(ck.base,ck.len,ck.from,ck.to,warm_user,on_event);
   }
  }
  if(tl.denseMax)dense_note_index((tl.denseMax&~DENSE_USER)>>dn::ALIAS_BITS); // sizes the person numbers of the dense users
  dense_note_end(denseEvEnd,tl.evEnd);dense_note_end(denseClEnd,tl.clEnd);
  tl.slotWaits=eslotWaits;
 }
 // ---- Raw users in the event slots become what the line loop would have written (see DENSE_USER_RAW). ----
 // The ID table still takes new IDs here; a thread's part of the slots is one contiguous range.
 if(dparse.raw&&denseEv){
  const size_t evEnd=denseEvEnd.load();
  #pragma omp parallel num_threads(T)
  {
   const size_t t=(size_t)omp_get_thread_num();ThreadState& tl=tls[t];uint32_t dmax=0;
   const size_t from=evEnd*t/(size_t)T,to=evEnd*(t+1)/(size_t)T;
   if(hasV3)resolve_raw_v3(denseEv,from,to,tab,tl,dmax);else resolve_raw_plain(denseEv,from,to,tab,tl,dmax);
   if(dmax)dense_note_index((dmax&~DENSE_USER)>>dn::ALIAS_BITS);
  }
 }
 const uint32_t totalIds=tab.end_parallel();
 __mark("PARSE");
 if(getenv("SS_STATS")&&(denseEv||denseCl)){uint64_t w=0;for(int t=0;t<T;++t)w+=tls[t].slotWaits;
  cerr<<"STATS dense slots: event end "<<denseEvEnd.load()<<" click end "<<denseClEnd.load()<<" waits for a busy slot "<<w<<"\n";}
 if(dumpDir&&!dumpText){cerr<<"stopping after the parse stage\n";cerr.flush();_exit(0);}
 if(dumpDir){
  // Test only (fuzz/dense_check.py): rows-T.txt = what the general handlers saw (raw texts, written above),
  // recs-T.txt = every record of every bucket, slots.txt = every slot in use, ID texts rebuilt from the counters.
  auto user_text=[&](uint32_t user)->string{
   if(user&LOCAL_USER)return "L:"+to_string(user&~LOCAL_USER);
   if(user&DENSE_USER){char t[9];const uint64_t c=user&~DENSE_USER;
    const size_t n=dn::text36('u',dn::value_of(((c>>dn::ALIAS_BITS)<<dn::GEN_ALIAS_BITS)|(c&((1u<<dn::ALIAS_BITS)-1)),dense.ku),t);return "U:"+hexstr(string_view(t,n));}
   return "T:"+to_string(user);
  };
  fs::create_directories(dumpDir);
  for(int t=0;t<T;++t){
   {ofstream o(fs::path(dumpDir)/("rows-"+to_string(t)+".txt"),ios::binary);o<<tls[t].dumpRows;}
   ofstream o(fs::path(dumpDir)/("recs-"+to_string(t)+".txt"),ios::binary);
   unordered_map<const char*,int64_t> side;for(const auto& e:tls[t].ingestSide)side.emplace(e.first,e.second);
   for(const auto& v:tls[t].eb)for(size_t i=0;i<v.size();++i){const PRec& r=v[i];
    size_t n=0;while(r.id[n]!=','&&r.id[n]!='"')++n;
    const uint64_t c=r.ih>>PR_HBITS;const int64_t ing=c==INGEST_SIDE?side.at(r.id):(int64_t)(c+(uint64_t)ingestBase);
    o<<"E\tP\t"<<hexstr(string_view(r.id,n))<<'\t'<<ing<<'\t'<<r.k<<'\t'<<r.cat<<'\t'<<(r.k?user_text(r.user):string("-"))<<'\n';}
   for(const auto& v:tls[t].cb)for(size_t i=0;i<v.size();++i){const CRec& r=v[i];
    o<<"C\tP\t"<<hexstr(string_view(r.id,r.idlen))<<'\t'<<r.key<<'\t'<<user_text(r.user)<<'\n';}
   // bucket form: every dense row, with the ID text rebuilt from bucket index and record
   for(size_t b=0;b<tls[t].db.size();++b)for(size_t i=0;i<tls[t].db[b].size();++i){const DRec& r=tls[t].db[b][i];char tx[9];
    const size_t n=dn::text36('e',dn::value_of(((uint64_t)b<<dparse.shE)|r.lo,dense.ke),tx);
    o<<"E\tB\t"<<hexstr(string_view(tx,n))<<'\t'<<r.ingest<<'\t'<<r.k<<'\t'<<(r.k?r.cat:0)<<'\t'<<(r.k?user_text(r.user):string("-"))<<'\n';}
   for(size_t b=0;b<tls[t].dcb.size();++b)for(size_t i=0;i<tls[t].dcb[b].size();++i){const DCRec& r=tls[t].dcb[b][i];char tx[9];
    const size_t n=dn::text36('k',dn::value_of(r.kc,dense.kc),tx);
    if((r.kc>>dparse.shC)!=b)o<<"C\tWRONGBUCKET\t"<<hexstr(string_view(tx,n))<<'\n';
    o<<"C\tB\t"<<hexstr(string_view(tx,n))<<'\t'<<r.key<<'\t'<<user_text(r.user)<<'\n';}
  }
  {
   ofstream o(fs::path(dumpDir)/"slots.txt",ios::binary);
   // (only up to the reported ends: a slot written beyond them would be missing here and the check would say so)
   if(denseEv)for(uint64_t i=0;i<denseEvEnd.load();++i){const ESlot& r=denseEv[i];const uint64_t k=r.k.load(memory_order_relaxed);if(!k)continue;char tx[9];
    const size_t n=dn::text36('e',dn::value_of(i,dense.ke),tx);
    if(k==ESLOT_BUSY){o<<"E\tBUSY\t"<<hexstr(string_view(tx,n))<<'\n';continue;} // must not happen once parsing is done
    const bool valid=k!=ESLOT_INVALID;
    o<<"E\tD\t"<<hexstr(string_view(tx,n))<<'\t'<<r.ingest<<'\t'<<(valid?k:0)<<'\t'<<(valid?r.cat:0)<<'\t'<<(valid?user_text(r.user):string("-"))<<'\t'<<r.thread<<'\n';}
   if(denseCl)for(uint64_t i=0;i<denseClEnd.load();++i){const CSlot& r=denseCl[i];if(!r.present)continue;char tx[9];
    const size_t n=dn::text36('k',dn::value_of(i,dense.kc),tx);
    o<<"C\tD\t"<<hexstr(string_view(tx,n))<<'\t'<<r.key<<'\t'<<user_text(r.user)<<'\t'<<r.present-1<<'\n';}
  }
  { // users.txt: every ID-table entry (number, text), so that the check can name the person behind a "T:" number;
    // complex.txt: the user indexes that must not take the dense route.
   const auto R=memory_order_relaxed;
   ofstream o(fs::path(dumpDir)/"users.txt",ios::binary);
   for(size_t i=0;i<tab.cap;++i){const KeyTable::Slot& e=tab.slots[i];const uint32_t c=e.k1.load(R);
    if(!c||c==KeyTable::LONGK||c==KeyTable::BUSY)continue;
    char tx[12];const uint64_t k0=e.k0.load(R);const uint32_t k1=c&0xFFFFFFu;memcpy(tx,&k0,8);memcpy(tx+8,&k1,4);
    o<<e.v.load(R)<<'\t'<<hexstr(string_view(tx,(c>>24)-1))<<'\n';}
   for(const auto& l:tab.longs)o<<l.v<<'\t'<<hexstr(l.key)<<'\n';
   ofstream oc(fs::path(dumpDir)/"complex.txt",ios::binary);
   if(denseComplex)for(uint64_t i=0;i<dense.limI;++i)if(dense_complex(i))oc<<i<<'\n';
  }
  cerr<<"dense dump written to "<<dumpDir<<"; stopping after the parse stage\n";
  cerr.flush();_exit(0);
 }
 // SS_STATS=1 prints how well the hashes spread: table displacement (0 = key sits in its home slot),
 // bucket sizes, and tag matches between different IDs. Costs nothing when off.
 const bool stats=getenv("SS_STATS")!=nullptr;
 if(stats){
  uint64_t used=0,sum=0,mx=0;
  for(size_t j=0;j<tab.cap;++j){const uint32_t c=tab.slots[j].k1.load(memory_order_relaxed);if(!c)continue;
   const uint64_t k0=tab.slots[j].k0.load(memory_order_relaxed);
   const size_t home=(c==KeyTable::LONGK?k0:KeyTable::hash_packed(k0,c))&tab.mask,d=(j-home)&tab.mask;
   ++used;sum+=d;mx=max<uint64_t>(mx,d);}
  fprintf(stderr,"STATS user table: %llu of %llu slots used, displacement avg %.3f max %llu\n",(unsigned long long)used,(unsigned long long)tab.cap,used?(double)sum/used:0.0,(unsigned long long)mx);
  for(int kind=0;kind<2;++kind){
   const size_t P=kind?PC:PE;uint64_t tot=0,big=0,small=UINT64_MAX;
   for(size_t p=0;p<P;++p){uint64_t n=0;for(int t=0;t<T;++t)n+=kind?(p<tls[t].cb.size()?tls[t].cb[p].size():0):(p<tls[t].eb.size()?tls[t].eb[p].size():0);tot+=n;big=max(big,n);small=min(small,n);}
   fprintf(stderr,"STATS %s buckets: %llu, records %llu, mean %.1f min %llu max %llu\n",kind?"click":"event",(unsigned long long)P,(unsigned long long)tot,(double)tot/P,(unsigned long long)small,(unsigned long long)big);
  }
 }
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
 // Dense persons come after all table and fallback persons: denseBase + (user index<<3 | root alias).
 // (This is the BASE of DENSE.md; it has to clear the fallback numbers, so it is only known here.)
 const uint32_t denseBase=totalIds+(uint32_t)extraNames.size();
 // Name of a dense person: the byte-wise smallest text among the aliases grouped with its root alias.
 auto dense_name=[&](uint32_t person,char* buf)->size_t{
  const uint32_t d=person-denseBase,root=d&7;const uint64_t idx=d>>3;
  const uint32_t stored=denseComp[idx].load(memory_order_relaxed);
  if(__builtin_expect(stored==0,1))return text36_fast('u',dn::value_of((idx<<dn::GEN_ALIAS_BITS)|root,dense.ku),buf); // nothing grouped under this index
  const uint32_t w=stored^COMP_IDENT;uint64_t best=~0ull,bestV=0;
  for(uint32_t a=root;a<8;++a)if(comp_root(w,a)==root){ // the root is its group's smallest alias
   const uint64_t v=dn::value_of((idx<<dn::GEN_ALIAS_BITS)|a,dense.ku),key=order36(v);
   if(key<best){best=key;bestV=v;}
  }
  return text36_fast('u',bestV,buf);
 };
 auto name_of=[&](uint32_t person)->string_view{
  if(person<totalIds)return personNames[person].view();
  if(person>=denseBase){ // remembered per thread: a person's rows come one after another
   static thread_local char buf[16];static thread_local uint32_t who=UINT32_MAX;static thread_local size_t len=0;
   if(who!=person){len=dense_name(person,buf);who=person;}
   return string_view(buf,len);
  }
  return extraNames[person-totalIds];
 };
 // Ingest values that did not fit a record (see PRec::ih), by event id pointer. Normally empty.
 unordered_map<const char*,int64_t> sideIngest;
 for(int t=0;t<T;++t)for(const auto& e:tls[t].ingestSide)sideIngest.emplace(e.first,e.second);
 if(!sideIngest.empty())cerr<<"ingest values kept on the side: "<<sideIngest.size()<<"\n";
 auto ingest_ms=[&](const PRec& r)->int64_t{
  const uint64_t c=r.ih>>PR_HBITS;
  return c==INGEST_SIDE?sideIngest.find(r.id)->second:(int64_t)(c+(uint64_t)ingestBase);
 };
 const uint32_t newCount=totalIds-nlinked;
 cerr<<"unlinked IDs: "<<newCount<<" (+"<<extraNames.size()<<" via fallback)\n";
 // ---- Person buckets: bucket = person >> pshift, so a bucket is a dense range of W person numbers
 // and the last stage can group by person with one counting pass. Aim for about 16k events per bucket.
 // With dense users the number space runs up to the highest user index seen; most of it is empty.
 const uint64_t npersons=(uint64_t)denseBase+(denseUsers?((uint64_t)denseMaxIdx.load()+1)<<dn::ALIAS_BITS:0);
 if(npersons>=0xF0000000ull)throw runtime_error("Too many IDs");
 if(denseUsers&&getenv("SS_STATS"))cerr<<"STATS dense users: base "<<denseBase<<" max index "<<denseMaxIdx.load()<<" limit "<<dense.limI<<" person numbers "<<npersons<<"\n";
 // The user field of a parsed record -> person number.
 const uint32_t denseTop=DENSE_USER|(uint32_t)((((uint64_t)denseMaxIdx.load()+1)<<dn::ALIAS_BITS)-1); // highest valid DENSE_USER code
 // (Everything but `trans` is held by value: each thread works on its own copy, so the hot loops keep these in registers.)
 const auto person_of_shared=[&trans,comp=(const atomic<uint32_t>*)denseComp,denseBase,denseTop](uint32_t u,int t)->uint32_t{
  if(u&LOCAL_USER)return trans[t][u&~LOCAL_USER];
  if(u&DENSE_USER){
   if(__builtin_expect(u>denseTop,0)){fprintf(stderr,"ERROR dense user index above the reported maximum\n");_exit(1);}
   const uint32_t d=u&~DENSE_USER; // see dense_root
   return denseBase+(d^((comp[d>>3].load(memory_order_relaxed)>>(3*(d&7)))&7));
  }
  return u;
 };
 // Dense rows may arrive in per-thread buckets by counter range instead of in the slots (tls[t].db /
 // dcb, bucket = counter >> shift): then the DENSE1 stage below fills the slots from them.
 const bool bucketMode=dparse.bucket;
 const size_t nEvBuckets=bucketMode&&dparse.events?(size_t)(dense.limE>>dparse.shE)+1:0,nClBuckets=bucketMode&&dparse.clicks?(size_t)(dense.limC>>dparse.shC)+1:0;
 // The reader only needs the first 16 bytes of an event slot (k, user, cat, thread). When the slots are
 // filled here from buckets, the array holds just those (ESmall) and the winners' ingest times live in a
 // scratch array while a bucket is filled; ev_at() reads either layout.
 struct ESmall{uint64_t k;uint32_t user;uint16_t cat;uint16_t thread;};
 static_assert(sizeof(ESmall)==16&&offsetof(ESmall,user)==offsetof(ESlot,user)&&offsetof(ESmall,cat)==offsetof(ESlot,cat)&&offsetof(ESmall,thread)==offsetof(ESlot,thread)&&offsetof(ESlot,k)==0);
 const char* const evBase=nEvBuckets?(const char*)map_huge((size_t)(dense.limE+1)*sizeof(ESmall)):(const char*)denseEv;
 const unsigned evShift=nEvBuckets?4:5;
 const auto ev_at=[evBase,evShift](size_t c)->const ESmall&{return *(const ESmall*)(const void*)(evBase+(c<<evShift));};
 if(nClBuckets&&!denseCl)denseCl=(CSlot*)map_huge((size_t)(dense.limC+1)*sizeof(CSlot));
 const unsigned bshE=dparse.shE,bshC=dparse.shC;
 const auto db_size=[&](int t,size_t b)->size_t{return b<tls[t].db.size()?tls[t].db[b].size():0;};
 const auto dcb_size=[&](int t,size_t b)->size_t{return b<tls[t].dcb.size()?tls[t].dcb[b].size():0;};
 const bool slotMode=evBase||denseCl;
 if(slotMode)denseInfo=(DInfo*)map_huge(((size_t)denseMaxIdx.load()+2)*sizeof(DInfo));
 uint64_t parsedEvents=0,parsedClicks=0;
 size_t maxEvBucket=0,maxClBucket=0; // most parsed lines in one ID bucket (all threads together)
 for(size_t p=0;p<PE;++p){size_t n=0;for(int t=0;t<T;++t)if(p<tls[t].eb.size())n+=tls[t].eb[p].size();parsedEvents+=n;maxEvBucket=max(maxEvBucket,n);}
 for(size_t p=0;p<PC;++p){size_t n=0;for(int t=0;t<T;++t)if(p<tls[t].cb.size())n+=tls[t].cb[p].size();parsedClicks+=n;maxClBucket=max(maxClBucket,n);}
 // SS_PERSONBUCKET: events per person bucket to aim for (measuring switch; the last stage sorts one
 // bucket at a time in two buffers of 16 bytes per event, so this sets its working set).
 static const uint64_t perBucket=[]{const char* v=getenv("SS_PERSONBUCKET");const long long x=v?atoll(v):16384;return (uint64_t)(x<256?256:x>(1<<24)?(1<<24):x);}();
 // The person numbers come in two stretches that are used very differently, so each has its own
 // bucket width: table and fallback persons [0, denseBase) are numbered without gaps and get every
 // row of the general lane that is not a dense user's (2^shA persons per bucket, about perBucket events);
 // dense persons [denseBase, npersons) only come here when their user index could not take the fast
 // lane, so that stretch is mostly empty and is cut into at most a few hundred buckets (2^shB persons).
 // How many rows the first stretch gets from the slots is only known after the DENSE stage; a sample
 // of the event slots estimates it (a wrong guess costs time, nothing else).
 unsigned shA=0,shB=0;
 {uint64_t estA=parsedEvents;
  if(denseEv){const size_t end=denseEvEnd.load(),step=max<size_t>(1,end/1024);size_t taken=0,tableRows=0;
   for(size_t c=step/2;c<end;c+=step){++taken;const ESlot& sl=denseEv[c];if(sl.k.load(memory_order_relaxed)>=PR_KBIAS&&!(sl.user&DENSE_USER))++tableRows;}
   if(taken)estA+=(uint64_t)end*tableRows/taken;}
  if(nEvBuckets){ // rows still in buckets: the first record of every few buckets stands for its bucket
   const size_t step=max<size_t>(1,nEvBuckets*(size_t)T/2048);uint64_t all=0,tableRows=0,total=0;
   for(int t=0;t<T;++t)for(size_t b=0;b<tls[t].db.size();++b){const Bucket<DRec>& B=tls[t].db[b];const size_t n=B.size();total+=n;
    if(n&&(b+(size_t)t)%step==0){all+=n;if(!(B[0].user&DENSE_USER))tableRows+=n;}}
   if(all)estA+=total*tableRows/all;}
  const uint64_t want=max<uint64_t>(1,estA/perBucket);while(shA<16&&((uint64_t)denseBase>>shA)>want)++shA;
  while(shB<16&&((npersons-denseBase)>>shB)>256)++shB;}
 if(const char* v=getenv("SS_TEST_PSHIFT")){int s=atoi(v);if(s>=0&&s<=16)shA=shB=(unsigned)s;} // tests force odd bucket widths
 const uint32_t WA=1u<<shA,WB=1u<<shB,Wmax=max(WA,WB);
 const size_t nA=(size_t)((denseBase?denseBase-1:0)>>shA)+1; // buckets of the first stretch
 const size_t UPn=nA+(npersons>denseBase?(size_t)((npersons-denseBase-1)>>shB)+1:0);
 // Bucket of a person, and the tombstone number of that bucket (one past its last person).
 const auto bucket_of=[denseBase,shA,shB,nA](uint32_t p)->size_t{return p<denseBase?(size_t)(p>>shA):nA+((p-denseBase)>>shB);};
 const auto bucket_end=[denseBase,shA,shB](uint32_t p)->uint32_t{return p<denseBase?((p>>shA)+1)<<shA:denseBase+((((p-denseBase)>>shB)+1)<<shB);};
 // Survivors by person bucket, one list per thread and bucket (tls[t].uev/ucl are not used any more).
 struct Survivors{vector<EvList> ev;vector<ClickList> cl;HugePool pool;}; // pool: all of one thread's post-parse memory
 vector<Survivors> surv(T);
 // One region per thread should hold its share of the survivors plus its dedup tables; a thread that
 // does more than its share continues in smaller regions.
 // Measuring switches for the dedup stage.
 static const bool simdProbe=!getenv("SS_NOSIMDPROBE"); // first probe of eight slots at once
 static const size_t slotPct=[]{const char* v=getenv("SS_SLOTPCT");const long x=v?atol(v):200;return (size_t)(x<110?110:x>800?800:x);}(); // table slots per 100 records
 static const size_t pfe=[]{const char* v=getenv("SS_PFE");const long x=v?atol(v):128;return (size_t)(x<0?0:x>4096?4096:x);}(); // write prefetch this many bytes ahead of each survivor list's tail (0: only the line being written)
 const size_t slotCap=max(maxEvBucket,maxClBucket)*slotPct/100+2+4; // +4: the four-slot probe may read past the last slot
 for(int t=0;t<T;++t){surv[t].ev.resize(UPn);surv[t].cl.resize(UPn);tls[t].uev.clear();tls[t].ucl.clear();
  surv[t].pool.first=(size_t)((parsedEvents*sizeof(Ev)+parsedClicks*sizeof(Click))/(uint64_t)T*9/8)+slotCap*sizeof(uint32_t)+maxEvBucket*sizeof(uint32_t)+(size_t(2)<<20);
  surv[t].pool.next=max<size_t>(size_t(8)<<20,surv[t].pool.first/4);}
 // ---- Dedup each ID bucket on its own; survivors go to person buckets. ----
 uint64_t dedupCount=0,surviving=0,survivingClicks=0;
 uint64_t stEvN=0,stEvDisp=0,stEvMax=0,stEvFalse=0,stClN=0,stClDisp=0,stClMax=0,stClFalse=0; // SS_STATS (the two "False" counts are always kept: they cost nothing)
 #pragma omp parallel num_threads(T) reduction(+:dedupCount,surviving,survivingClicks,stEvN,stEvDisp,stEvFalse,stClN,stClDisp,stClFalse) reduction(max:stEvMax,stClMax)
 {
  const int tid=omp_get_thread_num();
  // With slots in use, a dense user whose row goes through the general lists is marked: the reader
  // then leaves all of that user index to the general lane.
  DInfo* const markInfo=slotMode?denseInfo:nullptr;
  EvList* const myEv=surv[tid].ev.data();ClickList* const myCl=surv[tid].cl.data();HugePool& pool=surv[tid].pool;
  // Slot = tag<<ib | (record number+1), 0 = empty; ib bits are enough for the bucket's record count.
  // The table has two slots per record; the low half of the hash picks the slot, the high half is the tag.
  Scratch<uint32_t> slotBuf,emitBuf;vector<size_t> off(T+1);
  slotBuf.pool=&pool;emitBuf.pool=&pool;slotBuf.reserve(slotCap);emitBuf.reserve(maxEvBucket);
  const bool probe4=simdProbe;const size_t ahead=pfe;const uint32_t tagMask=tagKeep&0xffffu; // plain locals for the hot loops
  const unsigned bshA=shA,bshB=shB;const size_t bnA=nA;const uint32_t bBase=denseBase;
  const auto person_of=person_of_shared; // this thread's copy
  auto bucket_size=[&](const auto& buckets,size_t p)->size_t{return p<buckets.size()?buckets[p].size():0;};
  // One pass per bucket: a record whose ID is new is passed on at once (if valid). When a later copy
  // turns out to be the earlier ingested one, the event already passed on is turned into a tombstone:
  // its person becomes the first number past its bucket's range, which the last stage never visits.
  #pragma omp for schedule(dynamic,1) nowait
  for(size_t p=0;p<PE;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=bucket_size(tls[t].eb,p);}off[T]=n;
   if(!n)continue;
   if(n>=0x7fffffffu){fprintf(stderr,"ERROR event ID bucket too large\n");_exit(1);}
   const size_t cap=n*slotPct/100+2;
   uint32_t* const S=slotBuf.get(cap+4);memset(S,0,cap*sizeof(uint32_t));memset(S+cap,0xff,4*sizeof(uint32_t)); // the pad is never empty
   uint32_t* const emitted=emitBuf.get(n); // where record g's event went: its list mark (only read for valid winners)
   const unsigned ib=(unsigned)bit_width(n);const uint32_t imask=(1u<<ib)-1;
   const __m128i vkeep=_mm_set1_epi32((int)~imask),vzero=_mm_setzero_si128();
   size_t dups=0,dead=0;
   for(int t=0;t<T;++t){
    const size_t m=bucket_size(tls[t].eb,p);if(!m)continue;
    const PRec* r=tls[t].eb[p].data();const PRec* const rend=r+m;uint32_t g1=(uint32_t)off[t]+1;
    for(;r!=rend;++r,++g1){
     // 41 hash bits per record: the low 25 pick the slot, the other 16 are the tag.
     const uint64_t ih=r->ih;const uint32_t hl=(uint32_t)(ih&PR_HMASK);
     const uint32_t tg=((uint32_t)r->hx&tagMask)<<ib;
     size_t j=(size_t)(((uint64_t)hl*cap)>>PR_HBITS);
     // First look at four slots at once: which are empty, and which carry this record's tag? Almost
     // always the first empty one comes before any such tag, and the record goes there without a
     // data-dependent loop. Everything else (and a run into the never-empty pad) goes slot by slot.
     unsigned mz=0,mt=1;
     if(probe4){
      const __m128i a=_mm_loadu_si128((const __m128i*)(S+j));
      mz=(unsigned)_mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(a,vzero)));
      mt=(unsigned)_mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(_mm_and_si128(a,vkeep),_mm_set1_epi32((int)tg))));
     }
     const unsigned any=mz|mt; // quick if the lowest slot that is empty or carries the tag is an empty one
     if(__builtin_expect(((any&(0u-any))&mz)!=0,1))j+=(unsigned)__builtin_ctz(mz);
     else for(uint32_t e;(e=S[j])!=0;){
      if((e^tg)<=imask){ // same tag: compare the IDs themselves
       const uint32_t og=(e&imask)-1;int ot=0;while(og>=off[ot+1])++ot;
       const PRec& o=tls[ot].eb[p][og-off[ot]];
       if(same_event_id(o.id,r->id)){
        // Keep the earliest ingested copy; copies with equal ingest_ts are identical.
        ++dups;
        const uint64_t ri=ih>>PR_HBITS,oi=o.ih>>PR_HBITS;
        const bool notEarlier=__builtin_expect((ri==INGEST_SIDE)|(oi==INGEST_SIDE),0)?ingest_ms(*r)>=ingest_ms(o):ri>=oi;
        if(notEarlier)goto next_event;
        if(o.k){const uint32_t ou=person_of(o.user,ot);
         Ev* const gone=myEv[bucket_of(ou)].at_mark(emitted[og]);gone->person=bucket_end(ou);++dead;}
        break;
       }
       ++stEvFalse; // same tag, other ID
      }
      if(++j==cap)j=0;
     }
     S[j]=tg|g1;
     {
      const uint64_t rk=r->k;
      if(rk){
       // user, cat and the tag bits sit in the record as the event's second word does (person, cat, unused).
       uint64_t w;memcpy(&w,(const void*)&r->user,8);
       size_t bkt;
       if((uint32_t)w&(LOCAL_USER|DENSE_USER)){
        const uint32_t ru=(uint32_t)w,pn=person_of(ru,t);
        w=(w&~0xffffffffull)|pn;
        if(ru&DENSE_USER){ // a dense person: second stretch
         bkt=bnA+((pn-bBase)>>bshB);
         if(markInfo)markInfo[(ru&~DENSE_USER)>>dn::ALIAS_BITS].general.store(1,memory_order_relaxed);
        }
        else bkt=pn>>bshA;
       }
       else bkt=(uint32_t)w>>bshA; // a table number: first stretch
       EvList& L=myEv[bkt];Ev* const out=L.slot(pool);
       emitted[g1-1]=L.mark(out);
       __builtin_prefetch((const char*)out+ahead,1,3); // every time: testing for a new cache line would be an unpredictable branch
       out->k=rk-PR_KBIAS; // otherwise already in its final form
       memcpy((void*)&out->person,&w,8);
      }
     }
     next_event:;
    }
   }
   if(stats)for(size_t j=0;j<cap;++j){const uint32_t e=S[j];if(!e)continue;
    const uint32_t og=(e&imask)-1;int ot=0;while(og>=off[ot+1])++ot;
    const size_t home=(size_t)(((uint64_t)(uint32_t)(tls[ot].eb[p][og-off[ot]].ih&PR_HMASK)*cap)>>PR_HBITS),d=j>=home?j-home:j+cap-home;
    ++stEvN;stEvDisp+=d;stEvMax=max<uint64_t>(stEvMax,d);}
   for(int t=0;t<T;++t)if(p<tls[t].eb.size())tls[t].eb[p]=Bucket<PRec>();
   surviving-=dead;dedupCount+=n-dups;
  }
  // Copies of a click are identical, so the first one seen is kept.
  #pragma omp for schedule(dynamic,1)
  for(size_t p=0;p<PC;++p){
   size_t n=0;for(int t=0;t<T;++t){off[t]=n;n+=bucket_size(tls[t].cb,p);}off[T]=n;
   if(!n)continue;
   if(n>=0x7fffffffu){fprintf(stderr,"ERROR click ID bucket too large\n");_exit(1);}
   const size_t cap=n*slotPct/100+2;
   uint32_t* const S=slotBuf.get(cap+4);memset(S,0,cap*sizeof(uint32_t));memset(S+cap,0xff,4*sizeof(uint32_t));
   const unsigned ib=(unsigned)bit_width(n);const uint32_t imask=(1u<<ib)-1;
   const __m128i vkeep=_mm_set1_epi32((int)~imask),vzero=_mm_setzero_si128();
   for(int t=0;t<T;++t){
    const size_t m=bucket_size(tls[t].cb,p);if(!m)continue;
    const CRec* r=tls[t].cb[p].data();const CRec* const rend=r+m;uint32_t g1=(uint32_t)off[t]+1;
    for(;r!=rend;++r,++g1){
     const uint64_t h=r->h;const uint32_t tg=(uint32_t)(h>>32)<<ib;
     size_t j=(size_t)(((h&0xffffffffu)*cap)>>32);
     unsigned mz=0,mt=1; // four slots at once, as for events
     if(probe4){
      const __m128i a=_mm_loadu_si128((const __m128i*)(S+j));
      mz=(unsigned)_mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(a,vzero)));
      mt=(unsigned)_mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(_mm_and_si128(a,vkeep),_mm_set1_epi32((int)tg))));
     }
     const unsigned any=mz|mt; // quick if the lowest slot that is empty or carries the tag is an empty one
     if(__builtin_expect(((any&(0u-any))&mz)!=0,1))j+=(unsigned)__builtin_ctz(mz);
     else for(uint32_t e;(e=S[j])!=0;){
      if((e^tg)<=imask){
       const uint32_t og=(e&imask)-1;int ot=0;while(og>=off[ot+1])++ot;
       const CRec& o=tls[ot].cb[p][og-off[ot]];
       if(o.idlen==r->idlen&&memcmp(o.id,r->id,r->idlen)==0)goto next_click;
       ++stClFalse;
      }
      if(++j==cap)j=0;
     }
     S[j]=tg|g1;
     {
      const uint32_t u=person_of(r->user,t);
      size_t bkt;
      if(r->user&DENSE_USER){bkt=bnA+((u-bBase)>>bshB);if(markInfo)markInfo[(r->user&~DENSE_USER)>>dn::ALIAS_BITS].general.store(1,memory_order_relaxed);}
      else bkt=u>>bshA;
      Click* const out=myCl[bkt].slot(pool);
      __builtin_prefetch((const char*)out+ahead,1,3);
      out->key=r->key;out->id=r->id;out->person=u;out->idlen=r->idlen;
     }
     next_click:;
    }
   }
   if(stats)for(size_t j=0;j<cap;++j){const uint32_t e=S[j];if(!e)continue;
    const uint32_t og=(e&imask)-1;int ot=0;while(og>=off[ot+1])++ot;
    const size_t home=(size_t)(((tls[ot].cb[p][og-off[ot]].h&0xffffffffu)*cap)>>32),d=j>=home?j-home:j+cap-home;
    ++stClN;stClDisp+=d;stClMax=max<uint64_t>(stClMax,d);}
   for(int t=0;t<T;++t)if(p<tls[t].cb.size())tls[t].cb[p]=Bucket<CRec>();
  }
 }
 if(stats)fprintf(stderr,"STATS event dedup: %llu IDs, displacement avg %.3f max %llu, tag matches of different IDs %llu\nSTATS click dedup: %llu IDs, displacement avg %.3f max %llu, tag matches of different IDs %llu\n",
  (unsigned long long)stEvN,stEvN?(double)stEvDisp/stEvN:0.0,(unsigned long long)stEvMax,(unsigned long long)stEvFalse,(unsigned long long)stClN,stClN?(double)stClDisp/stClN:0.0,(unsigned long long)stClMax,(unsigned long long)stClFalse);
 size_t maxEvPerson=0,maxClPerson=0; // most survivors in one person bucket
 for(size_t up=0;up<UPn;++up){size_t ne=0,nc=0;for(int t=0;t<T;++t){ne+=surv[t].ev[up].size();nc+=surv[t].cl[up].size();}
  surviving+=ne;survivingClicks+=nc;maxEvPerson=max(maxEvPerson,ne);maxClPerson=max(maxClPerson,nc);}
 cerr<<"deduplicated events: "<<dedupCount<<", surviving: "<<surviving<<"\n";
 cerr<<"surviving clicks: "<<survivingClicks<<"\n";
 __mark("DEDUP");
 // ---- DENSE stage, part 1: one sweep over the slots in counter order. ----
 // Rows of dense users are only counted: per user index the first and last counter, how many, and
 // which aliases (a user's rows sit next to each other, so the index's entry is updated once per run).
 // Rows of all other users go to the general person lists right here.
 const size_t evEnd=nEvBuckets?(size_t)dense.limE:evBase?denseEvEnd.load():0,clEnd=nClBuckets?(size_t)dense.limC:denseCl?denseClEnd.load():0;
 const bool slots=evEnd||clEnd;
 static constexpr size_t ECH=size_t(1)<<15,CCH=size_t(1)<<16,ICH=512; // slots / user indexes per piece of work
 // Test switches: every n-th user index is treated as if its rows were spread out (it takes the
 // sweep of part 3) / as if it could not take the fast lane (its rows are handed over in part 2).
 static const uint32_t noTightN=[]{const char* v=getenv("SS_TEST_NOTIGHT");return v?(uint32_t)max(1,atoi(v)):0u;}();
 static const uint32_t noFastN=[]{const char* v=getenv("SS_TEST_NOFAST");return v?(uint32_t)max(1,atoi(v)):0u;}();
 uint64_t rdSlotEv=0,rdSlotCl=0,rdHandEv=0,rdHandCl=0,rdFastIdx=0,rdFastEv=0,rdFastCl=0,rdDeferIdx=0; // SS_STATS
 // Who is handed over, in events (SS_STATS): rows of users that came through the ID table - of a linked
 // person, of an unlinked dense-looking ID with alias 8 or more, of an unlinked ID of a complex index, of
 // any other text - or through the fallback; then dense users whose index has rows in the general lane,
 // whose index holds several persons, whose index the test switch kept out, and whose rows are spread out.
 uint64_t rdWhy[9]={0,0,0,0,0,0,0,0,0};
 uint64_t fillEndE=0,fillEndC=0;
 // The general lane orders clicks of one moment by their ID text; a click from a slot gets its text made here.
 struct IdText{char* cur=nullptr;char* end=nullptr;
  const char* make(HugePool& pool,char letter,uint64_t value,uint32_t& len){
   if(end-cur<16){cur=(char*)pool.take(4096);end=cur+4096;}
   char* const t=cur;cur+=16;len=(uint32_t)text36_fast(letter,value,t);return t;}};
 const auto hand_event=[bucket_of](EvList* myEv,HugePool& pool,uint64_t k,uint32_t person,uint16_t cat){
  Ev* const out=myEv[bucket_of(person)].slot(pool);out->k=k-PR_KBIAS;out->person=person;out->cat=cat;out->pad=0;};
 const auto hand_click=[bucket_of,kc=dense.kc](ClickList* myCl,HugePool& pool,IdText& txt,uint64_t key,uint32_t person,uint32_t ctr){
  Click* const out=myCl[bucket_of(person)].slot(pool);out->key=key;out->person=person;out->id=txt.make(pool,'k',dn::value_of(ctr,kc),out->idlen);};
 if(slots){
  #pragma omp parallel num_threads(T) reduction(+:rdSlotEv,rdSlotCl,rdHandEv,rdHandCl,rdWhy[:9])
  {
   const auto why_table=[&](uint32_t u)->int{
    if(u&LOCAL_USER)return 4;
    if(u<nlinked)return 0;
    const string_view name=personNames[u].view();uint64_t idx;uint32_t alias;
    if(!dense_decode_user(name.data(),name.size(),idx,alias))return 3;
    return alias>=(1u<<dn::ALIAS_BITS)?1:2;};
   const int tid=omp_get_thread_num();
   EvList* const myEv=surv[tid].ev.data();ClickList* const myCl=surv[tid].cl.data();HugePool& pool=surv[tid].pool;
   DInfo* const info=denseInfo;IdText txt;
   const auto bad_user=[]{fprintf(stderr,"ERROR dense user index above the reported maximum\n");_exit(1);};
   // The sweep of one stretch of counters (events, then clicks).
   const auto sweep_events=[&](size_t lo,size_t hi){
    for(size_t c=lo;c<hi;){
     const ESmall& sl=ev_at(c);const uint64_t k=sl.k;
     if(k<PR_KBIAS){++c;continue;} // empty, or present but not valid
     const uint32_t u=sl.user;
     if(!(u&DENSE_USER)){hand_event(myEv,pool,k,(u&LOCAL_USER)?trans[sl.thread][u&~LOCAL_USER]:u,sl.cat);++rdHandEv;++rdSlotEv;++c;if(stats)++rdWhy[why_table(u)];continue;}
     if(__builtin_expect(u>denseTop,0))bad_user();
     // A run of this user index: up to the next valid slot of anybody else.
     const uint32_t code=u&~7u;const size_t first=c;size_t last=c;uint32_t cnt=1;
     for(++c;c<hi;++c){
      if(ev_at(c).k<PR_KBIAS)continue;
      if((ev_at(c).user&~7u)!=code)break;
      last=c;++cnt;
     }
     DInfo& f=info[(code&~DENSE_USER)>>dn::ALIAS_BITS];
     dense_note_first(f.ef,(uint32_t)first+1);dense_note_end(f.el,(uint32_t)last+1);f.ec.fetch_add(cnt,memory_order_relaxed);
     rdSlotEv+=cnt;
    }
   };
   const auto sweep_clicks=[&](size_t lo,size_t hi){
    for(size_t c=lo;c<hi;){
     const CSlot& sl=denseCl[c];
     if(!sl.present){++c;continue;}
     const uint32_t u=sl.user;
     if(!(u&DENSE_USER)){hand_click(myCl,pool,txt,sl.key,(u&LOCAL_USER)?trans[sl.present-1][u&~LOCAL_USER]:u,(uint32_t)c);++rdHandCl;++rdSlotCl;++c;continue;}
     if(__builtin_expect(u>denseTop,0))bad_user();
     const uint32_t code=u&~7u;const size_t first=c;size_t last=c;uint32_t cnt=1;
     for(++c;c<hi;++c){
      if(!denseCl[c].present)continue;
      if((denseCl[c].user&~7u)!=code)break;
      last=c;++cnt;
     }
     DInfo& f=info[(code&~DENSE_USER)>>dn::ALIAS_BITS];
     dense_note_first(f.cf,(uint32_t)first+1);dense_note_end(f.cl,(uint32_t)last+1);f.cc.fetch_add(cnt,memory_order_relaxed);
     rdSlotCl+=cnt;
    }
   };
   vector<int64_t> ingestScratch(bucketMode&&nEvBuckets?size_t(1)<<bshE:0);int64_t* const ingestOf=ingestScratch.data(); // ingest_ts of the winner in each slot of the bucket being filled (only read where the slot is taken)
   if(bucketMode){
    // DENSE1: bucket by bucket, the records of all threads go into the bucket's stretch of slots by
    // direct index. One thread owns the stretch, so nothing is atomic here. The copy with the smallest
    // ingest_ts stays (copies with equal ingest_ts are identical in defined data; the first one stays),
    // and a winner that is not a valid event is stored as ESLOT_INVALID. Then the stretch is swept at
    // once, while it is in the cache.
    #pragma omp for schedule(dynamic,1) nowait reduction(max:fillEndE)
    for(size_t b=0;b<nEvBuckets;++b){
     const size_t lo=b<<bshE;size_t any=0;
     for(int t=0;t<T;++t){const size_t n=db_size(t,b);if(!n)continue;
      any+=n;
      const DRec* r=tls[t].db[b].data();
      for(const DRec* const re=r+n;r!=re;++r){
       ESmall& sl=const_cast<ESmall&>(ev_at(lo|r->lo));
       if(sl.k==0||r->ingest<ingestOf[r->lo]){
        const uint64_t uct=eslot_uct(r->user,r->cat,(unsigned)t);
        sl.k=r->k?r->k:ESLOT_INVALID;memcpy((char*)&sl+offsetof(ESmall,user),&uct,8);ingestOf[r->lo]=r->ingest;
       }
      }
     }
     if(!any)continue; // nothing was written to this stretch
     const size_t hi=min(evEnd,lo+(size_t(1)<<bshE));
     fillEndE=max<uint64_t>(fillEndE,hi);
     sweep_events(lo,hi);
    }
    #pragma omp for schedule(dynamic,1) reduction(max:fillEndC)
    for(size_t b=0;b<nClBuckets;++b){
     const size_t lo=b<<bshC;size_t any=0;
     for(int t=0;t<T;++t){const size_t n=dcb_size(t,b);if(!n)continue;
      any+=n;
      const DCRec* r=tls[t].dcb[b].data();
      for(const DCRec* const re=r+n;r!=re;++r){
       if(__builtin_expect((r->kc>>bshC)!=b||r->kc>=dense.limC,0)){fprintf(stderr,"ERROR dense click record in the wrong bucket\n");_exit(1);}
       CSlot& sl=denseCl[r->kc];
       if(!sl.present){sl.key=r->key;sl.user=r->user;sl.present=(uint32_t)t+1;} // copies of a click are identical
      }
     }
     if(!any)continue;
     const size_t hi=min(clEnd,lo+(size_t(1)<<bshC));
     fillEndC=max<uint64_t>(fillEndC,hi);
     sweep_clicks(lo,hi);
    }
   }
   else{
    #pragma omp for schedule(dynamic,1) nowait
    for(size_t lo=0;lo<evEnd;lo+=ECH)sweep_events(lo,min(evEnd,lo+ECH));
    #pragma omp for schedule(dynamic,1)
    for(size_t lo=0;lo<clEnd;lo+=CCH)sweep_clicks(lo,min(clEnd,lo+CCH));
   }
  }
  if(bucketMode){dense_note_end(denseEvEnd,(uint32_t)fillEndE);dense_note_end(denseClEnd,(uint32_t)fillEndC);} // one past the last stretch written
  __mark("DENSE1");
 }
 const size_t idxEnd=slots&&denseUsers?(size_t)denseMaxIdx.load()+1:0;
 uint64_t deferLoE=UINT64_MAX,deferHiE=0,deferLoC=UINT64_MAX,deferHiC=0; // counter ranges holding the rows of deferred indexes
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
 const size_t ncells=ncamp*(size_t)ndays;
 const auto size_pools=[&]{const size_t need=ncells*(sizeof(Totals)+sizeof(uint64_t))+2*maxEvPerson*sizeof(Ev)+2*maxClPerson*sizeof(Click)+(maxEvPerson+1)*24+(size_t(50000)+(size_t)ndays)*48+(size_t(4)<<20);
  for(int t=0;t<T;++t)surv[t].pool.next=max<size_t>(size_t(8)<<20,need);}; // one more region per thread at most, unless names are very long
 size_pools();
 vector<Totals*> threadTotals(T,nullptr);vector<uint64_t*> threadAssists(T,nullptr);vector<string> dbg(debugCsv?T:0);
 // sessions and events of one row can never exceed the number of parsed event lines, so they are written
 // as 32-bit integers whenever that number fits (always, in practice); revenue stays 64-bit.
 const bool narrow=parsedEvents<0x7fffffffu&&!getenv("SS_TEST_WIDECOUNTS"); // tests force the 64-bit columns
 const int countType=narrow?PQ_INT32:PQ_INT64;
 const vector<PqCol> userCols={{"user_id",PQ_BYTES,false,CONV_UTF8},{"day",PQ_INT32,false,CONV_DATE},{"sessions",countType,false,CONV_NONE},
  {"events",countType,false,CONV_NONE},{"revenue_cents",PQ_INT64,false,CONV_NONE},{"top_category",PQ_BYTES,true,CONV_UTF8}};
 static const size_t pfa=[]{const char* v=getenv("SS_PFA");const long x=v?atol(v):128;return (size_t)(x<0?0:x>4096?4096:x);}(); // write prefetch this many bytes ahead in the two event counting passes (0 = off; 128 measured ~1% faster overall on the scoring machine)
 // Measuring switch, off by default: cut a person's events into cells in a separate pass without data-dependent
 // branches. On dev it trades about 29 more instructions per event for 0.35 fewer mispredicted branches.
 static const bool cellPass=getenv("SS_CELLPASS")!=nullptr;
 // Test switch: every person goes through sessionize_person (the function of the dense fast lane)
 // instead of the cell loop, which proves that function against the general lane on any data.
 static const bool personSess=getenv("SS_TEST_PERSONSESS")!=nullptr;
 // A person bucket with at most this many events (or fewer than half as many as it has person numbers)
 // is sorted and handled person by person instead of by the counting passes. SS_SMALLBUCKET=0 turns that off.
 static const size_t SMALL_BUCKET=[]{const char* v=getenv("SS_SMALLBUCKET");const long x=v?atol(v):4096;return (size_t)(x<0?0:x);}();
 if(personSess)cerr<<"test: every person through sessionize_person\n";
 uint64_t num_sessions=0;
 // Input files nobody reads any more are unmapped by the worker threads, a few large pieces each,
 // instead of leaving every page of them to process exit, which tears mappings down on one thread.
 vector<pair<char*,size_t>> unmapFirst,unmapLast;
 if(!getenv("SS_NOEARLYUNMAP")){ // measuring switch
  const bool eventsFree=!anyLongName.load()&&extraNames.empty(); // otherwise a person's name may still point into an event file
  group_pieces(eventFiles,(size_t)T,eventsFree?unmapFirst:unmapLast);
  group_pieces(clickFiles,(size_t)T,unmapLast); // click IDs still break ties while sessions are attributed
  group_pieces(linkFiles,1,unmapLast);
 }
 #pragma omp parallel num_threads(T) reduction(+:num_sessions,rdHandEv,rdHandCl,rdFastIdx,rdFastEv,rdFastCl,rdDeferIdx,rdWhy[:9])
 {
  const int tid=omp_get_thread_num();
  #pragma omp for schedule(static,1) nowait
  for(size_t i=0;i<unmapFirst.size();++i)munmap(unmapFirst[i].first,unmapFirst[i].second);
  // Per-thread campaign totals, laid out [day][campaign].
  // Everything this thread needs comes from its pool (fresh, so zeroed): totals, sort buffers, row buffers.
  HugePool& pool=surv[tid].pool;
  Totals* const tot=(Totals*)pool.take(ncells*sizeof(Totals));threadTotals[tid]=tot;
  uint64_t* const assists=(uint64_t*)pool.take(ncells*sizeof(uint64_t));threadAssists[tid]=assists;
  vector<uint32_t> freq(ncamp),positions(ncamp);vector<uint16_t> active(ncamp);size_t nact=0;
  vector<uint64_t> catsum(catnames.size());vector<uint16_t> touched;
  PqFile file;
  auto open_file=[&]{char name[32];snprintf(name,sizeof name,"part-%05d.parquet",tid);file.open(output/"user_daily"/name,userCols);};
  // Row buffers. A person adds at most ndays rows and the buffers are flushed between persons,
  // so the fixed-width columns never need a capacity check.
  static constexpr size_t FLUSH=50000;
  const size_t rowCap=FLUSH+(size_t)ndays;
  int32_t* const dayp=(int32_t*)pool.take(rowCap*4);int64_t* const revp=(int64_t*)pool.take(rowCap*8);
  int32_t* const sess32p=narrow?(int32_t*)pool.take(rowCap*4):nullptr;int32_t* const evs32p=narrow?(int32_t*)pool.take(rowCap*4):nullptr;
  int64_t* const sessp=narrow?nullptr:(int64_t*)pool.take(rowCap*8);int64_t* const evsp=narrow?nullptr:(int64_t*)pool.take(rowCap*8);
  uint8_t* const catbits=(uint8_t*)pool.take((rowCap+7)/8);
  // user_id bytes: room for rowCap short names; a longer run of long names moves to a bigger piece.
  size_t uidCap=rowCap*20+(size_t(1)<<16),uidLen=0,nrows=0;char* uidp=(char*)pool.take(uidCap);
  auto grow_uid=[&](size_t need){const size_t cap2=max(uidCap*2,uidLen+need);char* q=(char*)pool.take(cap2);memcpy(q,uidp,uidLen);uidp=q;uidCap=cap2;};
  string catv,catbody;
  auto flush_rows=[&]{
   if(!nrows)return;
   // top_category page: 1-bit definition levels as one bit-packed run, then the non-null values.
   const size_t n=nrows,groups=(n+7)/8;
   catbody.clear();
   string head;put_varint(head,(groups<<1)|1);
   put_le32(catbody,(uint32_t)(head.size()+groups));catbody+=head;catbody.append((const char*)catbits,groups);catbody+=catv;
   if(!file.is_open())open_file();
   file.row_group((int64_t)n,{string_view(uidp,uidLen),string_view((const char*)dayp,n*4),
    narrow?string_view((const char*)sess32p,n*4):string_view((const char*)sessp,n*8),
    narrow?string_view((const char*)evs32p,n*4):string_view((const char*)evsp,n*8),
    string_view((const char*)revp,n*8),catbody});
   memset(catbits,0,groups);catv.clear();uidLen=0;nrows=0;
  };
  Scratch<Ev> bufA,bufB;Scratch<Click> bufC,bufD;
  bufA.pool=bufB.pool=&pool;bufC.pool=bufD.pool=&pool;
  // One person's cells: earliest and latest key, the OR of the keys' low halves (purchase flag), and where the cell ends.
  struct Cell{uint64_t mn,mx;uint32_t anyLo,end;};
  static_assert(sizeof(Cell)==24&&offsetof(Cell,end)==offsetof(Cell,anyLo)+4);
  Scratch<Cell> cellBuf;cellBuf.pool=&pool;
  vector<uint32_t> thist((size_t)maxDigit+2),phist((size_t)Wmax+3),chist((size_t)Wmax+2);
  uint32_t* const th=thist.data();uint32_t* const pp=phist.data();uint32_t* const cp=chist.data();
  const bool shadow=personSess;
  // ---- One person at a time (the dense fast lane; SS_TEST_PERSONSESS=1 runs every person through it). ----
  // A person's name, looked up once: `fixed` names are at most 15 bytes inside a readable 16-byte block.
  struct NameRef{const char* p;uint32_t L;bool fixed;};
  // (The three lambdas hold what they can by value, so that they do not pin the cell loop's variables to memory.)
  auto person_name=[&dense_name,&extraNames,personNames,totalIds,denseBase](uint32_t person,char* tmp16)->NameRef{
   if(person<totalIds){const PName& n=personNames[person];const unsigned char inl=(unsigned char)n.b[15];
    if(inl!=0xFF)return {n.b,inl,true};
    const string_view v=n.view();return {v.data(),(uint32_t)v.size(),false};}
   if(person>=denseBase){memset(tmp16,0,16);return {tmp16,(uint32_t)dense_name(person,tmp16),true};}
   const string_view v=extraNames[person-totalIds];return {v.data(),(uint32_t)v.size(),false};
  };
  // Appends the finished (person, day) row; the day's category sums are in catsum/touched and are cleared.
  auto put_row=[&nrows,&uidCap,&uidLen,&uidp,&grow_uid,&touched,&catv,&dbg,&catnames,catsum=catsum.data(),dayp,revp,sess32p,evs32p,sessp,evsp,catbits,narrow,debugCsv,firstday,tid]
   (const NameRef& nm,int day,uint64_t dsess,uint64_t devs,uint64_t drev){
   uint16_t top=0;uint64_t best=0;
   if(!touched.empty()){
    for(uint16_t c:touched){if(catsum[c]>best||(catsum[c]==best&&catsum[c]>0&&catnames[c]<catnames[top])){top=c;best=catsum[c];}catsum[c]=0;}
    touched.clear();
   }
   const size_t r=nrows++;
   if(uidCap-uidLen<(size_t)nm.L+64)grow_uid((size_t)nm.L+64);
   {char* o=uidp+uidLen;memcpy(o,&nm.L,4);if(nm.fixed)memcpy(o+4,nm.p,16);else memcpy(o+4,nm.p,nm.L);uidLen+=4+nm.L;}
   dayp[r]=(int32_t)(firstday+day);revp[r]=(int64_t)drev;
   if(narrow){sess32p[r]=(int32_t)dsess;evs32p[r]=(int32_t)devs;}else{sessp[r]=(int64_t)dsess;evsp[r]=(int64_t)devs;}
   if(best!=0){catbits[r>>3]|=(uint8_t)(1u<<(r&7));put_bytes(catv,catnames[top]);}
   if(debugCsv){string& o=dbg[tid];
    o+=hexstr(string_view(nm.p,nm.L));o+=',';o+=to_string(firstday+day);o+=',';o+=to_string(dsess);o+=',';o+=to_string(devs);o+=',';o+=to_string(drev);o+=',';
    o+=best==0?string("NULL"):hexstr(catnames[top]);o+='\n';
   }
  };
  // All rows and campaign totals of one person. E[0..ne): the person's events in time order (ne > 0; the
  // order of events with the same time does not matter). cb..ce: the person's clicks in click_before order.
  // The rules are those of the cell loop below: a session starts at the first event and after every gap
  // of more than GAP; it belongs to the UTC day of its first event and is credited to the last click of
  // the seven days up to that event (both ends inclusive), every campaign clicked in those days gets an assist.
  auto sessionize_person=[&put_row,&touched,&num_sessions,&nrows,&flush_rows,catsum=catsum.data(),dOff=dayOff,nCamp=ncamp,org=organic,totals=tot,assist=assists,
   fr=freq.data(),pos=positions.data(),act=active.data()](const NameRef& nm,const Ev* E,size_t ne,const auto* cb,const auto* ce){
   size_t na=0; // the active-campaign list is empty between persons
   auto add=cb,expire=cb; // any click type with a `key`
   int currentday=-1;uint64_t dsess=0,drev=0,nsess=0;size_t rowStart=0; // rowStart: first event of the current row
   uint64_t prev=(E->k>>EV_TS)+1; // makes the first event start a session: its distance from `prev` wraps to 2^64-1
   Totals* cur=nullptr;bool purchasing=false;
   const Ev* const Eend=E+ne;
   for(const Ev* e=E;e<Eend;++e){
    const uint64_t x=e->k,ts=x>>EV_TS;
    if(__builtin_expect(ts-prev>(uint64_t)GAP,0)){
     const int d=(int)((ts+dOff)/(uint64_t)DAY);
     if(d!=currentday){
      const size_t at=(size_t)(e-E);
      if(currentday>=0){put_row(nm,currentday,dsess,at-rowStart,drev);dsess=0;drev=0;}
      currentday=d;rowStart=at;
     }
     ++dsess;++nsess;
     const uint64_t addLim=(ts+(uint64_t)WEEK+1)<<16,expLim=ts<<16;
     while(add<ce&&add->key<addLim){const uint16_t c=(uint16_t)add->key;if(fr[c]++==0){pos[c]=(uint32_t)na;act[na++]=c;}++add;}
     while(expire<add&&expire->key<expLim){const uint16_t c=(uint16_t)expire->key;if(--fr[c]==0){const uint32_t at=pos[c];const uint16_t back=act[--na];act[at]=back;pos[back]=at;}++expire;}
     uint16_t credited=org;
     if(add>expire)credited=(uint16_t)add[-1].key;
     cur=&totals[(size_t)d*nCamp+credited];cur->sessions++;purchasing=false;
     uint64_t* const arow=assist+(size_t)d*nCamp;
     #pragma GCC unroll 4
     for(size_t q=0;q<na;++q)++arow[act[q]];
    }
    prev=ts;
    if(__builtin_expect((x&EV_PURCHASE)!=0,0)){const uint64_t price=x&EV_PRICE;const uint16_t cat=e->cat;
     drev+=price;cur->revenue+=price;
     if(!purchasing){purchasing=true;cur->purchasing_sessions++;}
     if(catsum[cat]==0)touched.push_back(cat);
     catsum[cat]+=price;
    }
   }
   const uint64_t devs=ne-rowStart;
   put_row(nm,currentday,dsess,devs,drev);
   for(size_t q=0;q<na;++q)fr[act[q]]=0;
   num_sessions+=nsess;
   if(nrows>=FLUSH)flush_rows();
  };
  // ---- DENSE stage, part 2: user index by user index. ----
  // An index takes the fast lane if none of its rows is in the general lane, its events and its clicks
  // each sit in a short stretch of counters, and all of them belong to one person: its rows are
  // picked out of those stretches, put in order and go through sessionize_person. A short-stretch
  // index that cannot take the fast lane has its rows handed to the general lists right here; an
  // index with a long stretch is only marked, and part 3 collects the rows of all such indexes in
  // one sweep over the counters between the first and the last of them.
  {
   EvList* const myEv=surv[tid].ev.data();ClickList* const myCl=surv[tid].cl.data();
   DInfo* const info=denseInfo;IdText txt;
   // A click of the fast lane: time and campaign as in Click, and its counter, which stands for its ID.
   struct FClick{uint64_t key;uint32_t user;uint32_t ctr;};
   Scratch<Ev> fastE;Scratch<FClick> fastC;fastE.pool=&pool;fastC.pool=&pool;
   const atomic<uint32_t>* const comp=denseComp;const uint32_t dBase=denseBase;
   #pragma omp for schedule(dynamic,1) reduction(min:deferLoE,deferLoC) reduction(max:deferHiE,deferHiC)
   for(size_t i0=0;i0<idxEnd;i0+=ICH){
    const size_t i1=min(idxEnd,i0+ICH);
    for(size_t ix=i0;ix<i1;++ix){
     DInfo& f=info[ix];
     const uint32_t ec=f.ec.load(memory_order_relaxed),cc=f.cc.load(memory_order_relaxed);
     if(!(ec|cc))continue;
     const bool general=f.general.load(memory_order_relaxed)!=0;
     if(!ec&&!general)continue; // clicks of people without any event: nothing to write
     const size_t ef=ec?f.ef.load(memory_order_relaxed)-1:0,el=ec?f.el.load(memory_order_relaxed):0; // [ef, el)
     const size_t cf=cc?f.cf.load(memory_order_relaxed)-1:0,cl=cc?f.cl.load(memory_order_relaxed):0;
     bool tight=el-ef<=4*(size_t)ec+64&&cl-cf<=4*(size_t)cc+64;
     if(noTightN&&ix%noTightN==0)tight=false;
     if(!tight){
      f.state.store(DSTATE_DEFER,memory_order_relaxed);++rdDeferIdx;
      if(ec){deferLoE=min<uint64_t>(deferLoE,ef);deferHiE=max<uint64_t>(deferHiE,el);}
      if(cc){deferLoC=min<uint64_t>(deferLoC,cf);deferHiC=max<uint64_t>(deferHiC,cl);}
      continue;
     }
     const uint32_t stored=comp[ix].load(memory_order_relaxed);
     const uint32_t code=DENSE_USER|(uint32_t)(ix<<dn::ALIAS_BITS); // the index's users: code + alias
     bool fast=!general;
     int why=5; // why the index is handed over, if it is
     if(noFastN&&ix%noFastN==0){if(fast)why=7;fast=false;}
     size_t n=0,m=0;Ev* E=nullptr;FClick* C=nullptr;uint32_t root=0;
     if(fast){
      // Pick the index's rows out of the two stretches. An event slot starts like an Ev (k, user, cat),
      // so its first 16 bytes are copied as they are, minus the k bias; the copy only counts if the
      // slot is a valid row of this index. `mask` collects the aliases in use.
      uint32_t mask=0;
      E=fastE.get((size_t)ec+1);
      const __m128i bias=_mm_set_epi64x(0,(long long)PR_KBIAS);
      for(size_t c=ef;c<el;++c){
       const ESmall& sl=ev_at(c);
       const __m128i v=_mm_loadu_si128((const __m128i*)(const void*)&sl);
       const uint64_t k=(uint64_t)_mm_cvtsi128_si64(v);const uint32_t u=sl.user;
       _mm_storeu_si128((__m128i*)(void*)(E+n),_mm_sub_epi64(v,bias));
       const uint32_t ok=(uint32_t)(k>=PR_KBIAS)&(uint32_t)((u^code)<8);
       mask|=ok<<(u&7);n+=ok;
      }
      C=fastC.get((size_t)cc+1);
      for(size_t c=cf;c<cl;++c){
       const CSlot& sl=denseCl[c];const uint32_t u=sl.user;
       C[m].key=sl.key;C[m].user=u;C[m].ctr=(uint32_t)c;
       const uint32_t ok=(uint32_t)(sl.present!=0)&(uint32_t)((u^code)<8);
       mask|=ok<<(u&7);m+=ok;
      }
      if(__builtin_expect(n!=ec||m!=cc,0)){fprintf(stderr,"ERROR dense slots changed under the reader\n");_exit(1);}
      // One person: every alias in use has the same smallest alias of its group.
      const uint32_t a0=(uint32_t)__builtin_ctz(mask|0x100u);root=a0^((stored>>(3*a0))&7);
      if(mask&(mask-1))for(uint32_t a=a0+1;a<8;++a)if(((mask>>a)&1)&&(a^((stored>>(3*a))&7))!=root){fast=false;why=6;}
     }
     if(fast){
      const uint32_t person=dBase+(uint32_t)((ix<<dn::ALIAS_BITS)|root);
      ev_time_sort(E,E+n);
      // Clicks in click_before order: by time, and at the same moment the larger ID text first.
      // (They come in no particular order, so only short runs are sorted by insertion.)
      if(m>1){
       const auto before=[kc=dense.kc](const FClick& a,const FClick& b){const uint64_t ta=a.key>>16,tb=b.key>>16;
        return ta!=tb?ta<tb:order36(dn::value_of(a.ctr,kc))>order36(dn::value_of(b.ctr,kc));};
       if(m>16)sort(C,C+m,before);
       else for(FClick* x=C+1;x<C+m;++x){
        if(before(*x,x[-1])){const FClick v=*x;FClick* y=x;do{*y=y[-1];--y;}while(y>C&&before(v,y[-1]));*y=v;}
       }
      }
      char tmp[16];sessionize_person(person_name(person,tmp),E,n,C,C+m);
      ++rdFastIdx;rdFastEv+=n;rdFastCl+=m;
     }
     else{
      for(size_t c=ef;c<el;++c){const ESmall& sl=ev_at(c);const uint64_t k=sl.k;const uint32_t d=sl.user^code;
       if(k>=PR_KBIAS&&d<8){hand_event(myEv,pool,k,dBase+(uint32_t)((ix<<dn::ALIAS_BITS)|(d^((stored>>(3*d))&7))),sl.cat);++rdHandEv;++rdWhy[why];}}
      for(size_t c=cf;c<cl;++c){const CSlot& sl=denseCl[c];const uint32_t d=sl.user^code;
       if(sl.present&&d<8){hand_click(myCl,pool,txt,sl.key,dBase+(uint32_t)((ix<<dn::ALIAS_BITS)|(d^((stored>>(3*d))&7))),(uint32_t)c);++rdHandCl;}}
     }
    }
   }
   // ---- DENSE stage, part 3: the rows of the deferred indexes, in one sweep. ----
   #pragma omp for schedule(dynamic,1) nowait
   for(size_t lo=min<uint64_t>(deferLoE,deferHiE);lo<deferHiE;lo+=ECH){
    const size_t hi=min<size_t>(deferHiE,lo+ECH);
    for(size_t c=lo;c<hi;++c){const ESmall& sl=ev_at(c);const uint64_t k=sl.k;const uint32_t u=sl.user;
     if(k>=PR_KBIAS&&(u&DENSE_USER)){const uint32_t d=u&~DENSE_USER;
      if(info[d>>dn::ALIAS_BITS].state.load(memory_order_relaxed)==DSTATE_DEFER){hand_event(myEv,pool,k,dBase+(d^((comp[d>>3].load(memory_order_relaxed)>>(3*(d&7)))&7)),sl.cat);++rdHandEv;++rdWhy[8];}}}
   }
   #pragma omp for schedule(dynamic,1)
   for(size_t lo=min<uint64_t>(deferLoC,deferHiC);lo<deferHiC;lo+=CCH){
    const size_t hi=min<size_t>(deferHiC,lo+CCH);
    for(size_t c=lo;c<hi;++c){const CSlot& sl=denseCl[c];const uint32_t u=sl.user;
     if(sl.present&&(u&DENSE_USER)){const uint32_t d=u&~DENSE_USER;
      if(info[d>>dn::ALIAS_BITS].state.load(memory_order_relaxed)==DSTATE_DEFER){hand_click(myCl,pool,txt,sl.key,dBase+(d^((comp[d>>3].load(memory_order_relaxed)>>(3*(d&7)))&7)),(uint32_t)c);++rdHandCl;}}}
   }
   // The general lists are complete now: size the buffers of the rest of this stage by them.
   #pragma omp single
   {
    if(slots){
     maxEvPerson=0;maxClPerson=0;
     for(size_t up=0;up<UPn;++up){size_t ne=0,nc=0;for(int t=0;t<T;++t){ne+=surv[t].ev[up].size();nc+=surv[t].cl[up].size();}
      maxEvPerson=max(maxEvPerson,ne);maxClPerson=max(maxClPerson,nc);}
     size_pools();
     __mark("DENSE");
    }
   }
  }
  bufA.reserve(maxEvPerson);bufB.reserve(maxEvPerson);bufC.reserve(maxClPerson);bufD.reserve(maxClPerson);
  if(cellPass)cellBuf.reserve(maxEvPerson+2);
  Cell* const cells=cellBuf.p;
  #pragma omp for schedule(dynamic,1)
  for(size_t up=0;up<UPn;++up){
   size_t nE=0,nC=0;
   for(int t=0;t<T;++t){nE+=surv[t].ev[up].size();nC+=surv[t].cl[up].size();}
   if(!nE)continue;
   const uint32_t base=up<nA?(uint32_t)(up<<shA):denseBase+(uint32_t)((up-nA)<<shB),W=up<nA?WA:WB;
   if(SMALL_BUCKET&&nE<=max<size_t>(SMALL_BUCKET,W/2)){
    // Few rows for the bucket's width: sort them by person and time and go person by person, so the
    // work follows the rows and not the width (the counting passes below touch every person number).
    Ev* const A=bufA.get(nE);size_t n=0;const uint32_t tomb=base+W;
    for(int t=0;t<T;++t){const EvList& L=surv[t].ev[up];
     for(const EvList::Block* bl=L.head;bl;bl=bl->next){const Ev* s=bl->items;const size_t m=L.count(bl);
      for(size_t i=0;i<m;++i){A[n]=s[i];n+=s[i].person!=tomb;}}}
    if(!n)continue;
    sort(A,A+n,[](const Ev& a,const Ev& b){return a.person!=b.person?a.person<b.person:a.k<b.k;});
    Click* const CA=bufC.get(nC+1);size_t m=0;
    for(int t=0;t<T;++t){const ClickList& L=surv[t].cl[up];
     for(const ClickList::Block* bl=L.head;bl;bl=bl->next){const size_t k=L.count(bl);memcpy((void*)(CA+m),(const void*)bl->items,k*sizeof(Click));m+=k;}}
    if(m>1)sort(CA,CA+m,[](const Click& a,const Click& b){return a.person!=b.person?a.person<b.person:click_before(a,b);});
    size_t c0=0;
    for(size_t i=0;i<n;){
     const uint32_t person=A[i].person;size_t j=i+1;while(j<n&&A[j].person==person)++j;
     while(c0<m&&CA[c0].person<person)++c0;
     size_t c1=c0;while(c1<m&&CA[c1].person==person)++c1;
     char tmp[16];sessionize_person(person_name(person,tmp),A+i,j-i,(const Click*)CA+c0,(const Click*)CA+c1);
     c0=c1;i=j;
    }
    continue;
   }
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
     if(pfa)
     #pragma GCC unroll 4
     for(size_t i=0;i<m;++i){const __m128i e=_mm_loadu_si128((const __m128i*)(s+i));Ev* const d=A+th[((uint64_t)_mm_cvtsi128_si64(e)>>sh0)&m0]++;__builtin_prefetch((const char*)d+pfa,1,3);_mm_storeu_si128((__m128i*)d,e);}
     else
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
    if(pfa)
    #pragma GCC unroll 4
    for(size_t i=0;i<nE;++i){const __m128i e=_mm_loadu_si128((const __m128i*)(A+i));Ev* const d=B+pq[A[i].person-base]++;__builtin_prefetch((const char*)d+pfa,1,3);_mm_storeu_si128((__m128i*)d,e);}
    else
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
    if(shadow){ // test: this person through the per-person function instead of the cell loop
     const size_t n=ee-es;Ev* const P=A; // A is free once the events are grouped by person in B
     memcpy((void*)P,(const void*)(E+es),n*sizeof(Ev));
     ev_time_sort(P,P+n);
     char tmp[16];sessionize_person(person_name(person,tmp),P,n,cb,ce);
     continue;
    }
    const Click *add=cb,*expire=cb;
    int currentday=-1;uint64_t dsess=0,devs=0,drev=0;
    Totals* cur=nullptr;bool purchasing=false;uint64_t prevmax=0;
    // Cells: events are ordered by cell, so a cell ends at the first key at or past its upper bound.
    // With the cell pass the person's cells are first written out by a loop without data-dependent
    // branches (every event updates the current cell's entry; a new cell just moves to the next entry).
    const Cell* nextCell=cells+1; // entry 0 is a dummy the first event steps over
    if(cellPass){
     Cell* c=cells;uint64_t cellEnd=0,mn=0,mx=0,any=0;
     for(size_t i=es;i<ee;++i){
      const uint64_t x=E[i].k;const uint64_t nw=0-(uint64_t)(x>=cellEnd); // all ones if x starts a new cell
      c=(Cell*)((char*)c+(nw&sizeof(Cell)));
      cellEnd^=(cellEnd^(((x>>CSH)+1)<<CSH))&nw;
      mn=min(mn|nw,x);mx=max(mx&~nw,x);any=(any&~nw)|x; // written with masks: a new cell must not be a branch
      c->mn=mn;c->mx=mx;const uint64_t tail=(uint64_t)(uint32_t)any|((uint64_t)(i+1)<<32);memcpy((void*)&c->anyLo,&tail,8); // anyLo and end in one store
     }
    }
    for(size_t a=es;;){
     // One cell: earliest and latest time, and whether it holds a purchase.
     uint64_t mn=0,mx=0,any=0;size_t b=a;bool fresh=false;int d=currentday;
     if(a<ee){
      if(cellPass){mn=nextCell->mn;mx=nextCell->mx;any=nextCell->anyLo;b=nextCell->end;++nextCell;}
      else{
       mn=E[a].k;mx=mn;any=mn;const uint64_t cellEnd=((mn>>CSH)+1)<<CSH;
       for(b=a+1;b<ee;++b){const uint64_t x=E[b].k;if(x>=cellEnd)break;mn=min(mn,x);mx=max(mx,x);any|=x;}
      }
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
       if(uidCap-uidLen<64)grow_uid(64);
       unsigned char inl=0xFF;if(person<totalIds)inl=(unsigned char)personNames[person].b[15];
       if(inl!=0xFF){char* o=uidp+uidLen;const uint32_t L=inl;memcpy(o,&L,4);memcpy(o+4,personNames[person].b,16);uidLen+=4+L;}
       else{const string_view name=name_of(person);const uint32_t L=(uint32_t)name.size();
        if(uidCap-uidLen<(size_t)L+64)grow_uid((size_t)L+64);
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
      uint64_t* const arow=assists+(size_t)d*ncamp;
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
  // Every row is in the output buffers now (names are copied there): nothing points into an input file.
  #pragma omp for schedule(static,1) nowait
  for(size_t i=0;i<unmapLast.size();++i)munmap(unmapLast[i].first,unmapLast[i].second);
  flush_rows();
  if(tid==0&&!file.is_open())open_file(); // the folder always holds at least one file, even with no rows
  if(file.is_open())file.close();
 }
 vector<Totals> campTotals(ncamp*(size_t)ndays);
 for(const Totals* tt:threadTotals)if(tt)for(size_t i=0;i<ncells;++i){campTotals[i].sessions+=tt[i].sessions;campTotals[i].purchasing_sessions+=tt[i].purchasing_sessions;campTotals[i].revenue+=tt[i].revenue;}
 for(const uint64_t* ta:threadAssists)if(ta)for(size_t i=0;i<ncells;++i)campTotals[i].assists+=ta[i];
__mark("AGGREGATE");
 if(slots&&stats){
  fprintf(stderr,"STATS dense reader: slots %llu events %llu clicks, fast lane %llu indexes %llu events %llu clicks, handed over %llu events %llu clicks, deferred indexes %llu\n",
   (unsigned long long)rdSlotEv,(unsigned long long)rdSlotCl,(unsigned long long)rdFastIdx,(unsigned long long)rdFastEv,(unsigned long long)rdFastCl,(unsigned long long)rdHandEv,(unsigned long long)rdHandCl,(unsigned long long)rdDeferIdx);
  fprintf(stderr,"STATS handed-over events: table users %llu linked person, %llu alias 8+, %llu complex index (unlinked ID), %llu other text, %llu fallback; dense users %llu general-lane mark, %llu several persons, %llu test switch, %llu spread out\n",
   (unsigned long long)rdWhy[0],(unsigned long long)rdWhy[1],(unsigned long long)rdWhy[2],(unsigned long long)rdWhy[3],(unsigned long long)rdWhy[4],(unsigned long long)rdWhy[5],(unsigned long long)rdWhy[6],(unsigned long long)rdWhy[7],(unsigned long long)rdWhy[8]);
  fprintf(stderr,"STATS person buckets: %zu of 2^%u table persons, %zu of 2^%u dense persons\n",nA,shA,UPn-nA,shB);
 }
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
 // Every output file is written and closed. Leave at once: unwinding would only hand gigabytes of
 // mappings back piece by piece before the kernel drops them anyway.
 cerr.flush();fflush(nullptr);
 _exit(0);
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
