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
#include <sys/resource.h>
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
 // All entries are dir/name with the same dir, so comparing the whole strings orders them exactly as comparing the
 // paths does (which walks both paths element by element on every comparison). SS_PATHSORT=path: the path comparison.
 static const bool pathSort=getenv("SS_PATHSORT")&&string_view(getenv("SS_PATHSORT"))=="path";
 if(pathSort)sort(out.begin(),out.end());
 else sort(out.begin(),out.end(),[](const fs::path& a,const fs::path& b){return a.native()<b.native();});
 return out;
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
// ---- Switches for the kernel's share of the run (system time). All are off by default and none can change
// the output: they only change where the input's pages sit in the address space and when they are mapped.
// The active ones are listed in the "STATS kernel switches" line (SS_STATS=1).
//  SS_FILEALIGN=2m      every input file starts at a 2 MB boundary of the reservation (default: 4 KB). File
//                       offsets and addresses are then congruent modulo 2 MB, which the kernel requires before it
//                       maps a 2 MB folio of the page cache with one PMD entry (one fault and one rmap step per
//                       2 MB instead of 32 faults and 512 page-table entries). Costs address space only.
//                       SS_FILEALIGN=2m+huge also advises MADV_HUGEPAGE on the file mappings.
//  SS_POPULATE=ahead    each parser thread asks for its next 2 MB chunk with one madvise(MADV_POPULATE_READ)
//                       before it parses it (one system call instead of ~32 fault traps per chunk).
//  SS_POPULATE=all      all threads populate the whole input right after it is mapped (inside the MAP stage).
//  SS_POPULATE=bg[n]    n (default 2) extra threads populate the input in the background from the end of MAP on;
//                       they stop when the parse stage ends.
//  SS_FILEADVISE=random|seq  MADV_RANDOM / MADV_SEQUENTIAL on the file mappings (after MADV_WILLNEED). Either one
//                       tells the kernel the mapping says nothing about page recency, so unmapping skips the
//                       per-page folio_mark_accessed step; seq may also change read-ahead decisions.
//  SS_SMAPS=1          measuring only: after PARSE, print Rss / Anonymous / AnonHugePages / FilePmdMapped of the
//                       process (SMAPS lines) - FilePmdMapped > 0 means SS_FILEALIGN=2m took effect.
//  Older ones: SS_NOWILLNEED=1 (no MADV_WILLNEED on the files), SS_NOEARLYUNMAP=1 (inputs and slot arrays stay
//  mapped until exit), SS_NOHUGE=1 (no MADV_HUGEPAGE on anonymous memory), SS_PREFAULT=1 (MADV_POPULATE_WRITE on
//  the arenas), SS_ASYNCFREE=1.
#ifndef MADV_POPULATE_READ
#define MADV_POPULATE_READ 22
#endif
static int optFileAlign=2; // 0 = 4 KB (SS_FILEALIGN=off), 1 = 2 MB, 2 = 2 MB + MADV_HUGEPAGE (default: lets the kernel map 2 MB page-cache folios with one entry)
static int optFileAdvise=0; // 0 none, 1 MADV_RANDOM, 2 MADV_SEQUENTIAL
static int optPopulate=0,optPopulateBg=2; // 0 off, 1 ahead, 2 all, 3 bg
static vector<Mapped> map_files(const vector<fs::path>& paths,FileGroup& g){
 const size_t HUGE2M=size_t(2)<<20;
 const auto slot_of=[&](size_t len){const size_t s=((len+4095)&~size_t(4095))+4096;return optFileAlign?(s+HUGE2M-1)&~(HUGE2M-1):s;};
 vector<size_t> lens(paths.size(),0);size_t total=0;
 for(size_t i=0;i<paths.size();++i){
  struct stat st{};if(stat(paths[i].c_str(),&st)!=0)throw runtime_error("Cannot stat "+paths[i].string());
  lens[i]=(size_t)st.st_size;if(lens[i])total+=slot_of(lens[i]);
 }
 vector<Mapped> out(paths.size());
 if(!total)return out;
 void* base=mmap(nullptr,total+(optFileAlign?HUGE2M:0),PROT_READ,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
 if(base==MAP_FAILED)throw runtime_error("Cannot reserve address space for the input files");
 if(optFileAlign)base=(void*)(((uintptr_t)base+HUGE2M-1)&~(uintptr_t)(HUGE2M-1)); // the unused edges stay reserved, without pages
 g.base=(char*)base;g.len=total;size_t off=0;
 // MADV_WILLNEED walks the page cache of the whole file (measured: ~15 us per 11 MB file with everything cached,
 // more than half of the MAP stage). By default a background thread per folder makes those calls, so read-ahead of
 // files that are not cached still starts at once while this thread goes on. SS_WILLNEED=inline: this thread makes
 // them, file by file, as before. SS_NOWILLNEED=1: no such calls at all.
 static const int willNeed=getenv("SS_NOWILLNEED")?0:(getenv("SS_WILLNEED")&&string_view(getenv("SS_WILLNEED"))=="inline")?1:2;
 vector<pair<void*,size_t>> later;
 // Where each file goes is fixed first, so the files can be opened and mapped by several threads at once (the calls
 // of one file take ~8 us and a run has a few hundred files): SS_MAPTHREADS=n, n = 2..16 threads for folders of 16
 // files or more. Default 1 (this thread alone, as before): not measurable on the loaded two-core dev box.
 static const int mapThreads=[]{const char* v=getenv("SS_MAPTHREADS");const int x=v?atoi(v):1;return x<1?1:x>16?16:x;}();
 vector<size_t> offs(paths.size(),0);
 for(size_t i=0;i<paths.size();++i){if(!lens[i])continue;offs[i]=off;off+=slot_of(lens[i]);g.ends.push_back(off);}
 atomic<size_t> nextFile{0},badOpen{SIZE_MAX},badMap{SIZE_MAX};
 const auto note_min=[](atomic<size_t>& a,size_t i){size_t c=a.load();while(i<c&&!a.compare_exchange_weak(c,i)){}};
 const auto map_some=[&]{
  for(;;){
   const size_t i=nextFile.fetch_add(1,memory_order_relaxed);if(i>=paths.size())break;
   if(!lens[i])continue;
   int fd=open(paths[i].c_str(),O_RDONLY);
   if(fd<0){note_min(badOpen,i);continue;}
   void* got=mmap(g.base+offs[i],lens[i],PROT_READ,MAP_PRIVATE|MAP_FIXED,fd,0);
   close(fd);
   if(got==MAP_FAILED){note_min(badMap,i);continue;}
   if(willNeed==1)madvise(got,lens[i],MADV_WILLNEED);
   if(optFileAlign==2)madvise(got,lens[i],MADV_HUGEPAGE);
   if(optFileAdvise)madvise(got,lens[i],optFileAdvise==1?MADV_RANDOM:MADV_SEQUENTIAL);
   out[i]={g.base+offs[i],lens[i]};
  }
 };
 {
  vector<thread> helpers;
  if(paths.size()>=16)for(int t=1;t<mapThreads;++t)helpers.emplace_back(map_some);
  map_some();
  for(thread& h:helpers)h.join();
 }
 // The first file that failed, in the order of the list, is the one reported (open before map, as one thread would).
 {const size_t bo=badOpen.load(),bm=badMap.load();
  if(bo!=SIZE_MAX&&bo<bm)throw runtime_error("Cannot open "+paths[bo].string());
  if(bm!=SIZE_MAX)throw runtime_error("Cannot map "+paths[bm].string());}
 if(willNeed==2)for(size_t i=0;i<paths.size();++i)if(lens[i])later.push_back({(void*)out[i].base,lens[i]});
 // Advice only: it cannot change what is read. (A call that came after its file was unmapped would fail or hint
 // at whatever is mapped there by then; the calls are done long before anything is unmapped.)
 if(!later.empty())thread([list=move(later)]{for(const auto& [p,n]:list)madvise(p,n,MADV_WILLNEED);}).detach();
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
// SS_STATS: per-thread clocks of the reader stages (see the "STATS dense threads" line). optThrStats is set once, before any thread works.
static bool optThrStats=false;
static thread_local double tlGrowWall=0;static thread_local uint32_t tlGrowCalls=0;
static inline double now_s(){timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (double)t.tv_sec+1e-9*(double)t.tv_nsec;}
static inline double thr_cpu_s(){timespec t;clock_gettime(CLOCK_THREAD_CPUTIME_ID,&t);return (double)t.tv_sec+1e-9*(double)t.tv_nsec;}
struct alignas(128) ThrStat{double in=0,cIn=0,first=-1,cFirst=0,last=0,cLast=0,out=0,cOut=0,wWall=0,wMax=0,wCpu=0,oWall=0,gWall=0,endWall=0;uint32_t wCalls=0,gCalls=0;};
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
  const double t0=optThrStats?now_s():0;
  cur=(char*)map_huge(want);lim=cur+want;
  if(optThrStats){tlGrowWall+=now_s()-t0;++tlGrowCalls;}
 }
 void ensure(size_t bytes){if((size_t)(lim-cur)<bytes)grow(bytes);} // room for `bytes` without another mapping
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
 // A key that does not fit a slot (over 11 bytes). kind 0: any text (a = pointer into a mapped input, b = length).
 // kind != 0: a CANONICAL LONG ID, one byte + exactly 32 of [0-9a-f] (GFAST.md: one ID in eight): a = the last 16 hex
 // digits as a number, b = the first 16, kind = 0x100 | the first byte. Text and (kind, a, b) are one-to-one, so
 // such keys are compared as two words and never as text. The slot holds a hash (k0) and the index of the entry.
 struct Long{uint64_t a,b;uint32_t v;uint32_t kind;
  string str()const{if(!kind)return string((const char*)a,(size_t)b);
   static const char* const HX="0123456789abcdef";string t(33,(char)(uint8_t)kind);for(int i=0;i<16;++i){t[1+i]=HX[(b>>(60-4*i))&15];t[17+i]=HX[(a>>(60-4*i))&15];}return t;}
 };
 // The entries: appended one at a time in serial mode (grows), by index from an atomic counter in lock-free mode
 // (room for every ID the mode may add is mapped by begin_parallel, untouched until used).
 struct LongList{
  Long* d=nullptr;atomic<size_t> n{0};size_t room=0;void* raw=nullptr;size_t rawLen=0;
  ~LongList(){if(raw)munmap(raw,rawLen);}
  size_t size()const{return min(n.load(memory_order_relaxed),room);}
  Long& operator[](size_t i){return d[i];}const Long& operator[](size_t i)const{return d[i];}
  Long* begin(){return d;}Long* end(){return d+size();}const Long* begin()const{return d;}const Long* end()const{return d+size();}
  void reserve(size_t want){if(want<=room)return;void* r;size_t rl;Long* const nd=(Long*)map_huge(want*sizeof(Long),&r,&rl);
   const size_t have=size();if(have)memcpy((void*)nd,(const void*)d,have*sizeof(Long));if(raw)munmap(raw,rawLen);d=nd;raw=r;rawLen=rl;room=want;}
  void push_back(const Long& l){const size_t i=size();if(i==room)reserve(max<size_t>(1024,room*2));d[i]=l;n.store(i+1,memory_order_relaxed);}
 };
 // Rewrites the value of the long entries [from, to) (to <= longs.size()); disjoint ranges may run in different
 // threads while nobody adds keys (the links stage sets every linked ID to its group's root this way).
 template<class FN> void remap_long(size_t from,size_t to,FN&& fn){for(size_t i=from;i<to;++i)longs.d[i].v=fn(longs.d[i].v);}
 // Sixteen hex digits [0-9a-f] at p as a number (first digit highest); false if any byte is something else.
 __attribute__((always_inline)) static bool hex16(const char* p,uint64_t& v){
  const __m128i x=_mm_loadu_si128((const __m128i*)(const void*)p);
  const __m128i dg=_mm_sub_epi8(x,_mm_set1_epi8('0')),al=_mm_sub_epi8(x,_mm_set1_epi8('a'));
  const __m128i isd=_mm_cmpeq_epi8(_mm_min_epu8(dg,_mm_set1_epi8(9)),dg),isa=_mm_cmpeq_epi8(_mm_min_epu8(al,_mm_set1_epi8(5)),al);
  if(_mm_movemask_epi8(_mm_or_si128(isd,isa))!=0xFFFF)return false;
  const __m128i nib=_mm_or_si128(_mm_and_si128(isd,dg),_mm_and_si128(isa,_mm_add_epi8(al,_mm_set1_epi8(10))));
  const __m128i w=_mm_or_si128(_mm_slli_epi16(_mm_and_si128(nib,_mm_set1_epi16(0x00FF)),4),_mm_srli_epi16(nib,8)); // per pair of digits: first << 4 | second
  v=__builtin_bswap64((uint64_t)_mm_cvtsi128_si64(_mm_packus_epi16(w,w)));return true;
 }
 // The key of a text that pack() refused, and its hash for the slot.
 __attribute__((always_inline)) static uint64_t long_key(string_view s,Long& l){
  if(s.size()==33&&hex16(s.data()+1,l.b)&&hex16(s.data()+17,l.a)){l.kind=0x100u|(uint8_t)s[0];return mix64(l.a^mix64(l.b^((uint64_t)l.kind*0x9E3779B97F4A7C15ull)));}
  l.a=(uint64_t)(uintptr_t)s.data();l.b=s.size();l.kind=0;return hkey(s);
 }
 __attribute__((always_inline)) static bool same_long(const Long& x,const Long& y){
  if(x.kind!=y.kind)return false;
  if(x.kind)return ((x.a^y.a)|(x.b^y.b))==0;
  return x.b==y.b&&memcmp((const void*)(uintptr_t)x.a,(const void*)(uintptr_t)y.a,(size_t)x.b)==0;
 }
 static constexpr uint32_t NONE=UINT32_MAX,BUSY=0xFE000000u,LONGK=0xFF000000u;
 Slot* slots=nullptr;size_t cap=0,mask=0,count=0;LongList longs;void* slotsMap=nullptr;size_t slotsMapLen=0;
 // lock-free mode: new keys get idBase+0, idBase+1, ... up to idLimit keys; newNames keeps their text
 atomic<uint32_t> nextId{0};uint32_t idBase=0,idLimit=0;PName* newNames=nullptr;void* namesMap=nullptr;size_t namesMapLen=0;
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
  Long key;const uint64_t h=long_key(s,key);
  for(size_t j=h&mask;;j=(j+1)&mask){Slot& e=slots[j];uint32_t c=e.k1.load(R);
   if(!c){e.k0.store(h,R);e.k1.store(LONGK,R);e.v.store((uint32_t)longs.size(),R);key.v=v;longs.push_back(key);++count;return v;}
   if(c==LONGK&&e.k0.load(R)==h&&same_long(longs[e.v.load(R)],key))return longs[e.v.load(R)].v;
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
  longs.reserve(longs.size()+(size_t)maxNew+1); // every new ID could be a long one
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
  Long key;const uint64_t h=long_key(s,key);
  for(size_t j=h&mask;;j=(j+1)&mask){const Slot& e=slots[j];const uint32_t c=e.k1.load(memory_order_acquire);
   if(!c)return NONE;
   if(c==LONGK&&e.k0.load(R)==h&&same_long(longs[e.v.load(R)],key))return longs[e.v.load(R)].v;
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
 // A key over 11 bytes, lock-free like the packed keys: the slot is claimed (BUSY), the entry written at an index
 // of its own, then the slot published. (Until GFAST this path held one mutex for all threads and compared texts:
 // with one ID in eight long, every thread queued here.)
 uint32_t find_or_add_long(string_view s){Long key;const uint64_t h=long_key(s,key);return find_or_add_long_key(key,h,s);}
 // The same with the key at hand: h = long_key(s, key) (a caller that asked for the slot h & mask ahead of time).
 uint32_t find_or_add_long_key(Long& key,const uint64_t h,string_view s){
  const auto R=memory_order_relaxed;
  for(size_t j=h&mask;;){
   Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
   if(c==LONGK){if(e.k0.load(R)==h){const Long& l=longs.d[e.v.load(R)];if(same_long(l,key))return l.v;}}
   else if(c==0){
    if(nextId.load(R)>=idLimit)return NONE;
    uint32_t expect=0;
    if(!e.k1.compare_exchange_strong(expect,BUSY,memory_order_acquire))continue;
    uint32_t id=nextId.fetch_add(1,R);
    if(id>=idLimit){e.k1.store(0,memory_order_release);return NONE;}
    const size_t li=longs.n.fetch_add(1,R); // below longs.room: at most idLimit IDs are added in this mode
    newNames[id].set(s);key.v=idBase+id;longs.d[li]=key;
    e.k0.store(h,R);e.v.store((uint32_t)li,R);e.k1.store(LONGK,memory_order_release);
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
// Keys "time << 24 | position" (positions ascending, n < 2^24, times below 2^40) into time order, positions
// ascending inside one moment: stable counting passes of 11 bits over the time bits in use. h: 2048 counters.
// Returns where the result is, K or K2.
static inline uint64_t* time_pos_sort(uint64_t* K,uint64_t* K2,size_t n,uint32_t* h){
 if(n<=64){sort(K,K+n);return K;}
 uint64_t any=0;for(size_t i=0;i<n;++i)any|=K[i];
 for(unsigned sh=24;sh<64&&(any>>sh)!=0;sh+=11){
  memset(h,0,2048*sizeof(uint32_t));
  for(size_t i=0;i<n;++i)++h[(K[i]>>sh)&2047];
  uint32_t at=0;for(size_t d=0;d<2048;++d){const uint32_t c=h[d];h[d]=at;at+=c;}
  for(size_t i=0;i<n;++i){const uint64_t x=K[i];K2[h[(x>>sh)&2047]++]=x;}
  swap(K,K2);
 }
 return K;
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
// ---- Stream store (BUCKETS.md, stage A): the rows of one parser thread, appended by bucket ----
// One instance per thread. A row of bucket b goes into the bucket's fill line (one cache line = 64 / sizeof(REC)
// records); the record that fills the line sends it out with ONE 64-byte non-temporal store into the bucket's current
// chunk, so a row costs no read of the memory it ends up in and every byte of it is written once, in sequence.
//   fill[b]   the fill line of bucket b
//   bs[b]    {w, n, first}: w = where the next full line goes (nullptr: the bucket needs a chunk), n = records
//            appended so far (the fill line holds n mod RPL of them), first = id of its first chunk (0: none)
//   chunks   chunk id - 1 -> {data, next id}; a chunk is 2^cs bytes of records and nothing else (64-byte aligned)
// The lines, the states and the `last` ids of all buckets lie in one mapping on 2 MB pages. Chunks are taken on
// demand from the thread's arena (map_huge blocks, bump pointer): with shuffled input all buckets fill at the same
// rate, so the chunks being written at any time are neighbours in the arena (about two rounds of buckets x chunk
// size: 10 MB = five 2 MB pages for 1,200 buckets and 4 KB chunks) although there are thousands of write positions.
// finish() must be called by the owning thread when its rows are done: partial lines go out with zero padding
// (not counted) and sfence makes the non-temporal stores visible to the readers. No put() after finish().
// Reading (any thread, after every writer's finish()):
//   count(b)                 records of this thread in bucket b
//   for_segments(b, f)       f(const REC* p, size_t n) once per chunk of bucket b in append order; p is 64-byte
//                            aligned, n <= records per chunk, the n add up to count(b)
// release(out) hands the arena's mappings over for munmap (the store is empty after it).
template<class REC> struct StreamStore{
 static_assert(sizeof(REC)<=64&&64%sizeof(REC)==0&&std::is_trivially_copyable_v<REC>);
 static constexpr unsigned RPL=64/sizeof(REC); // records per fill line
 struct BS{char* w;uint32_t n;uint32_t first;};
 struct Chunk{char* data;uint32_t next;uint32_t pad;};
 REC* fill=nullptr;BS* bs=nullptr;uint32_t* last=nullptr;size_t nb=0; // last[b]: id of the bucket's newest chunk
 unsigned cs=12;uintptr_t cmask=4095;size_t perChunk=4096/sizeof(REC);
 vector<Chunk> chunks;
 char* acur=nullptr;char* alim=nullptr;size_t firstBytes=size_t(8)<<20,nextBytes=size_t(8)<<20;
 vector<pair<char*,size_t>> maps; // every mapping of this store (for munmap)
 uint64_t arenaBytes=0;
 // nb_ buckets, chunks of 2^cs_ bytes (6..20), the arena's first block for about `expect` bytes of records.
 void init(size_t nb_,unsigned cs_,size_t expect){
  nb=nb_;cs=min(max(cs_,6u),20u);cmask=(uintptr_t(1)<<cs)-1;perChunk=(size_t(1)<<cs)/sizeof(REC);
  void* raw;size_t rawLen;const size_t lines=nb*64,states=nb*sizeof(BS);
  char* const m=(char*)map_huge(lines+states+nb*sizeof(uint32_t),&raw,&rawLen);maps.emplace_back((char*)raw,rawLen); // zero pages: every bucket starts empty
  fill=(REC*)m;bs=(BS*)(m+lines);last=(uint32_t*)(m+lines+states);
  firstBytes=expect+(nb<<cs)+(size_t(2)<<20);nextBytes=max<size_t>(size_t(8)<<20,firstBytes/4);
  chunks.reserve(firstBytes>>cs);
 }
 // A fresh chunk for bucket b, linked behind its last one.
 [[gnu::noinline]] char* grow(size_t b){
  if(__builtin_expect(acur==alim,0)){
   void* raw;size_t rawLen;const size_t want=((maps.size()>1?nextBytes:firstBytes)+(size_t(2)<<20)-1)&~((size_t(2)<<20)-1); // a multiple of 2 MB, so of the chunk size
   acur=(char*)map_huge(want,&raw,&rawLen);alim=acur+want;maps.emplace_back((char*)raw,rawLen);arenaBytes+=want;
  }
  char* const d=acur;acur+=size_t(1)<<cs;
  chunks.push_back(Chunk{d,0,0});const uint32_t id=(uint32_t)chunks.size();
  BS& s=bs[b];if(s.first)chunks[last[b]-1].next=id;else s.first=id;
  last[b]=id;return d;
 }
 // The fill line of bucket b goes out to the bucket's chunk (the caller has put its last record in place).
 inline __attribute__((always_inline)) void send_line(size_t b,BS& s){
  char* w=s.w;if(__builtin_expect(!w,0))w=grow(b);
  const __m128i* const l=(const __m128i*)(fill+b*RPL);
  _mm_stream_si128((__m128i*)w,_mm_load_si128(l));_mm_stream_si128((__m128i*)(w+16),_mm_load_si128(l+1));
  _mm_stream_si128((__m128i*)(w+32),_mm_load_si128(l+2));_mm_stream_si128((__m128i*)(w+48),_mm_load_si128(l+3));
  w+=64;s.w=((uintptr_t)w&cmask)?w:nullptr; // a full chunk: the next line asks for a new one
 }
 // One record into bucket b (plain x86-64 code: usable from every loop).
 inline __attribute__((always_inline)) void put(size_t b,const REC& r){
  BS& s=bs[b];const uint32_t n=s.n;s.n=n+1;const unsigned pos=n&(RPL-1);
  fill[b*RPL+pos]=r;
  if(pos==RPL-1)send_line(b,s);
 }
 // The same without a branch on "did this record fill the line?" (taken every other time in no order with shuffled
 // input): the record goes to its fill line and a line that became full is only NOTED (pend[cnt], cnt grows by one
 // then; room for one entry per put) and marked in the top bit of the bucket's count; send_noted() sends the noted
 // lines. A put that finds the mark (the bucket's line is full and still here: two records of one bucket between two
 // send_noted calls, rare) sends the line first. Between a put_noted and the send_noted behind it nothing else may
 // look at the store (put, finish, count, for_segments): the counts carry the mark until then.
 static constexpr uint32_t NOTED=0x80000000u;
 inline __attribute__((always_inline)) void put_noted(size_t b,const REC& r,uint32_t* pend,unsigned& cnt){
  BS& s=bs[b];uint32_t n=s.n;
  if(__builtin_expect((n&NOTED)!=0,0)){n&=~NOTED;s.n=n;send_line(b,s);}
  const unsigned pos=n&(RPL-1);const uint32_t full=(uint32_t)(pos==RPL-1);
  fill[b*RPL+pos]=r;s.n=(n+1)|(full<<31);
  pend[cnt]=(uint32_t)b;cnt+=full;
 }
 inline __attribute__((always_inline)) void send_noted(const uint32_t* pend,unsigned cnt){
  for(unsigned j=0;j<cnt;++j){const size_t b=pend[j];BS& s=bs[b];const uint32_t n=s.n;if(__builtin_expect((n&NOTED)!=0,1)){s.n=n&~NOTED;send_line(b,s);}}
 }
 void finish(){
  for(size_t b=0;b<nb;++b){BS& s=bs[b];const unsigned pos=s.n&(RPL-1);if(!pos)continue;
   REC* const line=fill+b*RPL;memset((void*)(line+pos),0,(RPL-pos)*sizeof(REC));
   char* w=s.w;if(!w)w=grow(b);
   memcpy(w,line,64);w+=64;s.w=((uintptr_t)w&cmask)?w:nullptr;}
  _mm_sfence();
 }
 size_t count(size_t b)const{return bs[b].n;}
 template<class F> inline __attribute__((always_inline)) void for_segments(size_t b,F&& f)const{
  size_t left=bs[b].n;uint32_t id=bs[b].first;
  while(left){const Chunk& c=chunks[id-1];const size_t n=min(left,perChunk);
   if(left>n){const char* const nx=chunks[c.next-1].data;__builtin_prefetch(nx);__builtin_prefetch(nx+64);__builtin_prefetch(nx+128);__builtin_prefetch(nx+192);} // the reader's next chunk is anywhere in the arena: its first lines are asked for a chunk ahead
   f((const REC*)c.data,n);left-=n;id=c.next;}
 }
 // The first lines of bucket b's first chunk (a reader that knows which bucket comes next asks for them early).
 inline void prefetch_first(size_t b)const{if(const uint32_t id=bs[b].first){const char* const d=chunks[id-1].data;__builtin_prefetch(d);__builtin_prefetch(d+64);}}
 uint64_t records()const{uint64_t r=0;for(size_t b=0;b<nb;++b)r+=bs[b].n;return r;}
 void release(vector<pair<char*,size_t>>& out){
  out.insert(out.end(),maps.begin(),maps.end());maps.clear();vector<Chunk>().swap(chunks);
  fill=nullptr;bs=nullptr;last=nullptr;nb=0;acur=alim=nullptr;
 }
};
// The record of a dense event row in a stream (stage A): the ESlot payload, with the low bits of the counter where
// the slot has `thread` (the stream's owner is the thread). THE layout lives here and in the three functions below
// (and in erec8_v5, the same packing for eight rows at once); nothing else may look inside a record.
//   k       as ESlot::k of a finished slot: ESLOT_INVALID (2) for a line that is not a valid event, else the event's k
//   ucl     user (bits 0-31, may be raw: DENSE_USER_RAW | n with the text in spare) | cat << 32 | lo << 48,
//           lo = counter & ((1 << sh) - 1); the counter is (bucket << sh) | lo, sh <= 16
//   ingest  exact ingest_ts in ms;  spare  as ESlot::spare
struct ERec{uint64_t k;uint64_t ucl;int64_t ingest;uint64_t spare;};
static_assert(sizeof(ERec)==32);
// uct = user | cat << 32 (eslot_uct with thread 0; bits 48-63 are dropped here).
static inline __attribute__((always_inline)) ERec erec_make(uint64_t kk,uint64_t uct,int64_t ingest,uint64_t spare,uint32_t lo){
 return ERec{kk,(uct&0x0000FFFFFFFFFFFFull)|((uint64_t)lo<<48),ingest,spare};}
static inline __attribute__((always_inline)) uint32_t erec_lo(const ERec& r){return (uint32_t)(r.ucl>>48);}
// ---- General route: event records in streams (GFAST.md package B; layout: agent P1's board note of 23:12) ----
// A parsed event line of the general route, appended to the stream of bucket (hash of the event key) >> (64 - gbits)
// of the parsing thread (ThreadState::gs / gl). The event_id is in the record as a CLASS and a KEY that are
// one-to-one with its text, so "same event_id" = same class and same key; no ID text is read again:
//   class 0 (GCLS_SHORT)  'e' + 0..8 bytes of [0-9a-z]: key = those bytes as a word, first byte lowest, zero padded
//   class 1 (GCLS_LONG)   'e' + exactly 32 of [0-9a-f]: GRecL, hi = the first 16 digits as a number, lo = the last 16
//   class 2 (GCLS_OTHER)  any other text: key = pointer to the text in the mapped input (ends at ',' or '"'):
//                         compare with same_event_id()
//   k       as PRec::k (0 = the line is not a valid event)
//   uc      user (bits 0-31; a LOCAL_USER number belongs to the thread that owns the stream) | cat << 32 | class << 48
//   ingest  exact ingest_ts in ms (no code, no side list)
// Rule for the reader: per (class, key) the copy with the smallest ingest wins; a winner with k == 0 is no event.
// The hash: gkey_hash_short(key) / gkey_hash_long(lo, hi) / hkey(text) for class 2; its top gbits are the bucket,
// the rest is free for the bucket's own table (recomputable from the record: one or two multiplies).
struct GRec{uint64_t key;uint64_t k;uint64_t uc;int64_t ingest;};
struct GRecL{uint64_t lo,hi;uint64_t k;uint64_t uc;int64_t ingest;uint64_t pad[3];};
static_assert(sizeof(GRec)==32&&sizeof(GRecL)==64);
static constexpr uint64_t GCLS_SHORT=0,GCLS_LONG=1,GCLS_OTHER=2;
static inline __attribute__((always_inline)) uint64_t grec_uc(uint32_t user,unsigned cat,uint64_t cls){return (uint64_t)user|((uint64_t)(uint16_t)cat<<32)|(cls<<48);}
template<class R> static inline __attribute__((always_inline)) uint32_t grec_user(const R& r){return (uint32_t)r.uc;}
template<class R> static inline __attribute__((always_inline)) unsigned grec_cat(const R& r){return (unsigned)(uint16_t)(r.uc>>32);}
template<class R> static inline __attribute__((always_inline)) unsigned grec_cls(const R& r){return (unsigned)(r.uc>>48);}
static constexpr uint64_t GKEY_SALT=0xD6E8FEB86659FD93ull;
static inline __attribute__((always_inline)) uint64_t gkey_hash_short(uint64_t key){return mix64(key^GKEY_SALT);}
static inline __attribute__((always_inline)) uint64_t gkey_hash_long(uint64_t lo,uint64_t hi){return mix64(lo^mix64(hi^GKEY_SALT));}
struct alignas(64) ThreadState{
 SmallTable users; // IDs that did not fit in the shared table
 vector<pair<const char*,int64_t>> ingestSide; // (event id pointer, ingest ms) of records whose ingest code is INGEST_SIDE
 vector<Bucket<PRec>> eb;vector<Bucket<CRec>> cb; // parsed lines by event/click ID hash
 uint32_t tid=0; // index of this thread's state (a LOCAL_USER number only means something together with it)
 deque<array<char,16>> rawNames; // texts of raw users that ended up with a LOCAL_USER number (they need a place that stays)
 vector<Bucket<DRec>> db;vector<Bucket<DCRec>> dcb; // dense rows in bucket form, by counter range (SS_DENSE_RECORDS=bucket)
 uint64_t slotWaits=0; // eslotWaits of this thread
 uint64_t clWin2=0,clGeneral=0; // click line code: lines read through three windows (click_long_fields), lines left to the general handler (SS_STATS)
 uint64_t clgRows=0,clgSlow=0; // cl8g click loop (general route): lines it took, and how many of them went through click_line_v3 (SS_STATS)
 uint64_t cl8Rows=0,cl8Slow=0; // cl8 click loop: lines it took, and how many of them went through click_line_v3 (SS_STATS)
 uint32_t evEnd=0,clEnd=0; // one past the highest event / click counter this thread wrote a slot for
 uint64_t laneFixE=0,laneFixC=0; // lane loops: line ends of event / click rows looked up with memchr (SS_STATS)
 uint64_t laneShort=0; // lane loops: groups with fewer than eight lanes at work (SS_STATS)
 uint64_t ev8Rows=0,ev8Slow=0; // ev8 loop: rows finished, and how many of them the kernel left to the line code (SS_STATS)
 uint64_t gevAblSum=0; // SS_ABL=norec
 uint64_t gevRows=0,gevSlow=0,gevChecked=0; // general event loop (ev8g kernel): rows, rows the kernel refused (line code), rows compared in check mode
 struct GCap{PRec prec;uint64_t cls,key,hi;int64_t ingest;bool has;};GCap* gcap=nullptr; // SS_GEV=check: the general handler stores its record here instead of committing it
 StreamStore<ERec> es; // SS_COMMIT=stream: this thread's dense event rows by bucket (counter >> dparse.bsh)
 StreamStore<GRec> gs;StreamStore<GRecL> gl; // SS_GCOMMIT=stream: this thread's general event rows by bucket (short / other IDs; long IDs)
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
// SPREAD3[m]: bit 3j is set where bit j of m is set (an alias mask laid over the 3-bit group fields of a denseComp word).
static constexpr array<uint32_t,256> SPREAD3=[]{array<uint32_t,256> a{};for(unsigned m=0;m<256;++m){uint32_t v=0;for(unsigned j=0;j<8;++j)if((m>>j)&1)v|=1u<<(3*j);a[m]=v;}return a;}();
// SPREAD8[m]: byte j is 1 where bit j of m is set (eight one-byte counters bumped by one add).
static constexpr array<uint64_t,256> SPREAD8=[]{array<uint64_t,256> a{};for(unsigned m=0;m<256;++m){uint64_t v=0;for(unsigned j=0;j<8;++j)if((m>>j)&1)v|=1ull<<(8*j);a[m]=v;}return a;}();
static inline size_t text36_fast(char letter,uint64_t y,char* out){
 unsigned n;const uint64_t v=digits36(y,n);
 out[0]=letter;memcpy(out+1,&v,8);return (size_t)n+1;
}
// ---- Dense route, the reader of the slot arrays (DENSE stage) ----
// What the reader knows about a user index: where its events and clicks sit in the slot arrays
// (ef/el, cf/cl: first and last counter + 1, 0 = none; ec/cc: how many), whether any of its rows
// went through the general lane instead, and what was decided for it. The sweep also leaves the
// session list of the index's first run of event slots: sess points at ns entries "start time << 24 |
// rows of the run before this session", one end entry with the run's row count, then the run's
// purchases (session << 40 | category << 24 | price) and an all-ones entry. amask: the aliases the
// run uses. The list is the whole index only if no second run was found (multi) and bit 31 of ns is
// clear (rows in time order, fewer than 2^24 of them).
struct DInfo{atomic<uint32_t> ef,el,ec,cf,cl,cc;atomic<uint8_t> general,state,multi,amask;atomic<uint32_t> ns;atomic<uint64_t*> sess;};
static_assert(sizeof(DInfo)==40);
static DInfo* denseInfo=nullptr; // highest reported user index + 2 entries, zero pages until touched
static constexpr uint8_t DSTATE_DEFER=1; // rows are handed to the general lane by the sweep after the index pass
// first-counter fields hold counter + 1, so that zero memory means "none".
static inline void dense_note_first(atomic<uint32_t>& first,uint32_t ctr1){
 uint32_t cur=first.load(memory_order_relaxed);
 while((cur==0||ctr1<cur)&&!first.compare_exchange_weak(cur,ctr1,memory_order_relaxed)){}
}
static size_t pow2_parts(uint64_t est){size_t p=1;while(p<2048&&p*32768<est)p<<=1;return p;}
struct Camp{string id,channel;};
// ---- Campaign lookup of the click loops: a complete table (SS_CAMP=hash: the KeyTable lookup as before) ----
// Every campaign_id of up to 7 bytes is in one table without collisions (hash and displace): the text and its
// length are one 64-bit key (the bytes as they stand, zero-padded, length in the top byte), key * mul picks a
// displacement by its bits 32.. and a slot by its top bits, slot ^ displacement holds the key and the campaign
// number. Since the table holds ALL campaigns of up to 7 bytes, a text of up to 7 bytes is a campaign exactly if
// the one entry it points at has its key: found and not found are both exact after one compare. Longer texts,
// and everything when the table could not be built, go through the KeyTable as before.
// SS_CAMP=check: both lookups for every row; the run stops if they ever differ.
// BEGIN camp table (fuzz/camp_test.cpp compiles the text between these marks)
struct CampEnt{uint64_t key,ci;}; // empty entry: key = ~0 (no key has a length byte of 255)
struct CampLook{const CampEnt* t=nullptr;const uint16_t* disp=nullptr;uint64_t mul=0,lim=0;uint32_t sh=0,mask=0;}; // lim: texts shorter than this use the table (8, or 0: table off)
static constexpr uint64_t CAMP_LOW[8]={0,0xFFULL,0xFFFFULL,0xFFFFFFULL,0xFFFFFFFFULL,0xFFFFFFFFFFULL,0xFFFFFFFFFFFFULL,0xFFFFFFFFFFFFFFULL};
static inline __attribute__((always_inline)) uint64_t camp_key(const char* s,size_t n){uint64_t w;__builtin_memcpy(&w,s,8);return (w&CAMP_LOW[n])|((uint64_t)n<<56);} // n <= 7; s may be read up to 8 bytes
static inline __attribute__((always_inline)) uint32_t camp_table_find(const CampLook& c,uint64_t key){
 const uint64_t h=key*c.mul;
 const CampEnt& e=c.t[(h>>c.sh)^c.disp[(uint32_t)(h>>32)&c.mask]];
 return e.key==key?(uint32_t)e.ci:UINT32_MAX;
}
struct CampTable{
 vector<CampEnt> t;vector<uint16_t> disp;CampLook look;size_t keys=0;uint64_t checked=0;
 // One attempt with multiplier mul and 2^b slots: false if two keys share bucket and slot or a bucket fits nowhere.
 bool place(const vector<CampEnt>& ks,unsigned b,uint64_t mul){
  const size_t S=(size_t)1<<b;const uint32_t mask=(uint32_t)(S-1);const unsigned sh=64-b;
  vector<uint32_t> cnt(S+1,0),ord(ks.size());
  for(const CampEnt& k:ks)++cnt[(((uint32_t)((k.key*mul)>>32))&mask)+1];
  for(size_t i=0;i<S;++i)cnt[i+1]+=cnt[i];
  {vector<uint32_t> at(cnt.begin(),cnt.end()-1);for(uint32_t i=0;i<ks.size();++i)ord[at[((uint32_t)((ks[i].key*mul)>>32))&mask]++]=i;}
  vector<uint32_t> bys(S);for(uint32_t i=0;i<S;++i)bys[i]=i;
  stable_sort(bys.begin(),bys.end(),[&](uint32_t x,uint32_t y){return cnt[x+1]-cnt[x]>cnt[y+1]-cnt[y];}); // biggest buckets first
  t.assign(S,CampEnt{~0ULL,0});disp.assign(S,0);
  size_t freeAt=0;
  for(const uint32_t bk:bys){
   const uint32_t lo=cnt[bk],hi=cnt[bk+1];if(lo==hi)break;
   if(hi-lo==1){ // a single key takes the next free slot
    while(t[freeAt].key!=~0ULL)++freeAt;
    const CampEnt& k=ks[ord[lo]];disp[bk]=(uint16_t)(((k.key*mul)>>sh)^freeAt);t[freeAt]=k;continue;
   }
   for(uint32_t i=lo;i<hi;++i)for(uint32_t j=i+1;j<hi;++j)if(((ks[ord[i]].key*mul)>>sh)==((ks[ord[j]].key*mul)>>sh))return false;
   size_t d=0;
   for(;d<S;++d){bool fits=true;for(uint32_t i=lo;i<hi&&fits;++i)fits=t[((ks[ord[i]].key*mul)>>sh)^d].key==~0ULL;if(fits)break;}
   if(d==S)return false;
   disp[bk]=(uint16_t)d;for(uint32_t i=lo;i<hi;++i)t[((ks[ord[i]].key*mul)>>sh)^d]=ks[ord[i]];
  }
  look.t=t.data();look.disp=disp.data();look.mul=mul;look.sh=sh;look.mask=mask;look.lim=8;
  return true;
 }
 // ks: (key, campaign number) of every campaign_id of up to 7 bytes, each key once. False: no table (look.lim stays 0).
 bool build(const vector<CampEnt>& ks){
  look=CampLook{};keys=ks.size();
  if(ks.size()>32768)return false;
  unsigned b=6;while(((size_t)1<<b)<ks.size())++b;
  for(;b<=16;++b){
   uint64_t x=0x9E3779B97F4A7C15ULL;
   for(int a=0;a<12;++a){x+=0x9E3779B97F4A7C15ULL;uint64_t z=x;z=(z^(z>>30))*0xBF58476D1CE4E5B9ULL;z=(z^(z>>27))*0x94D049BB133111EBULL;z^=z>>31;
    if(place(ks,b,z|1)){for(const CampEnt& k:ks)if(camp_table_find(look,k.key)!=(uint32_t)k.ci){look=CampLook{};return false;} return true;}}
  }
  look=CampLook{};return false;
 }
};
// END camp table
static CampTable campTable;
static bool campCheck=false; // SS_CAMP=check
[[gnu::noinline,gnu::cold]] static void camp_check(const char* s,size_t n,uint32_t ci){ // SS_CAMP=check: the table against the KeyTable answer ci
 if(n>=8||!campTable.look.t)return;
 CampLook c=campTable.look;c.lim=8;
 const uint32_t f=camp_table_find(c,camp_key(s,n));
 if(f!=ci){cerr<<"SS_CAMP=check: campaign text '"<<string_view(s,n)<<"' table "<<f<<" KeyTable "<<ci<<"\n";abort();}
 __atomic_fetch_add(&campTable.checked,1,__ATOMIC_RELAXED);
}
// The campaign number of the n bytes at s (which may be read up to 12, as KeyTable::pack does); false if it is no campaign.
static inline __attribute__((always_inline)) bool camp_find(const CampLook& c,const KeyTable& camps,const char* s,size_t n,uint32_t& ci){
 if(__builtin_expect(n<c.lim,1)){
  const uint64_t key=camp_key(s,n),h=key*c.mul;
  const CampEnt& e=c.t[(h>>c.sh)^c.disp[(uint32_t)(h>>32)&c.mask]];
  ci=(uint32_t)e.ci;return __builtin_expect(e.key==key,1);
 }
 uint64_t k0;uint32_t k1;const string_view camp(s,n);
 ci=__builtin_expect(KeyTable::pack(camp,k0,k1),1)?camps.find_packed(k0,k1):camps.find(camp);
 if(__builtin_expect(campCheck,0))camp_check(s,n,ci);
 return ci!=KeyTable::NONE;
}
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
// Output switches for measuring (both off by default; the files' bytes are the same either way):
//  SS_OUTOPEN=early  both output folders, every thread's user_daily part file and the campaign_daily file are
//                    created (and their first four bytes written) by a helper thread while the input is parsed;
//                    a worker takes its open file at its first row group. Files of threads that
//                    never write a row are removed again after the last stage.
//  SS_OUTWRITE=end   a file's row groups are kept in memory (one copy) and written by one writev when the file
//                    is closed, instead of one writev per row group in the middle of the index loop.
static bool optOutEarly=false,optOutEnd=false;
struct PqFile{
 struct Chunk{int64_t offset,size,nvalues;};
 struct Group{int64_t rows,bytes;vector<Chunk> cols;};
 int fd=-1;int64_t pos=0,rows=0;vector<PqCol> cols;vector<Group> groups;string buf;
 bool defer=false;vector<string> held; // SS_OUTWRITE=end: everything not yet written
 bool is_open()const{return fd>=0;}
 void writev_all(vector<struct iovec>& iov){
  size_t k=0;
  while(k<iov.size()){
   ssize_t w=::writev(fd,iov.data()+k,(int)min<size_t>(iov.size()-k,64));
   if(w<0){if(errno==EINTR)continue;throw runtime_error("Parquet write failed");}
   size_t left=(size_t)w;
   while(k<iov.size()&&left>=iov[k].iov_len){left-=iov[k].iov_len;++k;}
   if(left){iov[k].iov_base=(char*)iov[k].iov_base+left;iov[k].iov_len-=left;}
  }
 }
 void adopt(int f,vector<PqCol> c){cols=std::move(c);fd=f;defer=optOutEnd;pos=4;} // f: a fresh file open for writing that holds "PAR1" already
 void write_all(const char* p,size_t n){
  if(defer){held.emplace_back(p,n);return;}
  while(n){ssize_t w=::write(fd,p,n);if(w<0){if(errno==EINTR)continue;throw runtime_error("Parquet write failed");}p+=w;n-=(size_t)w;}
 }
 void open(const fs::path& path,vector<PqCol> c){
  cols=std::move(c);fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);
  if(fd<0)throw runtime_error("Cannot create "+path.string());
  defer=optOutEnd;write_all("PAR1",4);pos=4;
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
  if(defer){size_t tot=0;for(const auto& v:iov)tot+=v.iov_len;string keep;keep.reserve(tot);for(const auto& v:iov)keep.append((const char*)v.iov_base,v.iov_len);held.push_back(std::move(keep));}
  else writev_all(iov);
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
  if(defer){defer=false;vector<struct iovec> iov;iov.reserve(held.size());for(const string& h:held)if(!h.empty())iov.push_back({(void*)h.data(),h.size()});
   writev_all(iov);held.clear();held.shrink_to_fit();}
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
// keysearch_paste.h - AVX-512 form of the first round of dn::find_key (agent C2, 2026-11-21). Text to paste into main.cpp.
//
// INTEGRATION (for whoever merges it; names are those of /home/claude/wtB/main.cpp at db47b3e, branch agent-post)
//  1. Paste this file's text inside `namespace dn`, right before `static bool find_key(` (it uses dn::MASK and dn::I1 and
//     nothing else of the program; it needs <immintrin.h>, <cstdint>, <cstddef>).
//  2. In find_key, before `#pragma omp parallel`, add:      const bool ks5=ks_has512();
//  3. In the first-round branch `if(round==0&&limit<=MASK){`, before its line `const uint64_t D1=(u0^u1)<<24,...`, add:
//         if(ks5){const uint64_t c=ks_block_v5(um,n,limit,inc,b,e);
//          if(c!=UINT64_MAX){uint64_t cur=best.load(memory_order_relaxed);while(c<cur&&!best.compare_exchange_weak(cur,c)){}}
//          continue;}
//     Nothing else changes: the scalar loop below it stays for other CPUs, the second round and the limit > MASK case stay.
//     (vert/ks_test.cpp builds exactly this patched find_key from the production text and compares it with the original.)
//  Dispatch: ks_has512() = the CPU has avx512f and avx512dq (vpmullq); the scoring machine has both.
//
// CONTRACT  ks_block_v5(um, n, limit, inc, b, e) returns the smallest c in [b, e) that the first round accepts, or
// UINT64_MAX if there is none - exactly what the scalar loop of the first round does with one block [b, e): c is accepted
// iff sample 1 has a counter below limit under the key um[0] ^ ((c*inc) & MASK) and at most one of the samples 1..n-1 is
// at or above limit. Requires limit <= MASK (the branch's own condition) and n >= 2. ks_block_scalar is the scalar loop
// itself (same text as in find_key), for the test and as the definition.
// The vector loop tests 32 candidates per turn (four vectors of eight: one xor and one 64-bit multiply each, with the
// 40-bit numbers in the top 40 bits of the lanes as in the scalar code; then one unsigned compare of the lane-wise minimum
// of the four products with the limit). When any lane passes, the passing candidates are checked against all samples one
// by one in increasing c, so the first accepted c of the block is the scalar loop's. The last (e - b) mod 32 candidates
// run through the scalar loop.
#define DN_KS5 __attribute__((target("avx512f,avx512dq")))
static inline bool ks_has512(){static const bool h=__builtin_cpu_supports("avx512f")&&__builtin_cpu_supports("avx512dq");return h;}
// the full rule for one candidate (A = its c*inc in the top 40 bits): at most one of the samples 1..n-1 off
static inline __attribute__((always_inline)) bool ks_full(const uint64_t* um,size_t n,uint64_t limit,uint64_t A){
 const uint64_t k=um[0]^(A>>24);
 size_t miss=0;for(size_t i=1;i<n&&miss<2;++i)miss+=(((um[i]^k)*I1)&MASK)>=limit;
 return miss<2;
}
static inline uint64_t ks_block_scalar(const uint64_t* um,size_t n,uint64_t limit,uint64_t inc,uint64_t b,uint64_t e){
 const uint64_t D1=(um[0]^um[1])<<24,INC=inc<<24,LIM=limit<<24;uint64_t A=((b*inc)&MASK)<<24;
 for(uint64_t c=b;c<e;++c,A+=INC){
  if(__builtin_expect(((D1^A)*I1)>=LIM,1))continue; // sample 1 off
  if(ks_full(um,n,limit,A))return c;
 }
 return UINT64_MAX;
}
// the candidates of one turn that passed the one-sample test (bit j of m = candidate j, A = c*inc of candidate 0), in
// increasing c: the first that passes the full rule, as an offset, or UINT64_MAX. Out of line: it keeps the loop small.
__attribute__((noinline)) static uint64_t ks_hits(const uint64_t* um,size_t n,uint64_t limit,uint64_t A,uint64_t INC,uint32_t m){
 for(;m;m&=m-1){const unsigned j=(unsigned)__builtin_ctz(m);if(ks_full(um,n,limit,A+(uint64_t)j*INC))return j;}
 return UINT64_MAX;
}
DN_KS5 static uint64_t ks_block_v5(const uint64_t* um,size_t n,uint64_t limit,uint64_t inc,uint64_t b,uint64_t e){
 const uint64_t D1=(um[0]^um[1])<<24,INC=inc<<24,LIM=limit<<24;uint64_t A=((b*inc)&MASK)<<24,c=b;
 const __m512i vD=_mm512_set1_epi64((long long)D1),vI=_mm512_set1_epi64((long long)I1),vL=_mm512_set1_epi64((long long)LIM),v8=_mm512_set1_epi64((long long)(8*INC));
 __m512i a0=_mm512_add_epi64(_mm512_set1_epi64((long long)A),_mm512_mullo_epi64(_mm512_set1_epi64((long long)INC),_mm512_setr_epi64(0,1,2,3,4,5,6,7))); // lane j: A + j*INC, wrapping as the scalar A does
 for(;c+32<=e;c+=32,A+=32*INC){
  const __m512i a1=_mm512_add_epi64(a0,v8),a2=_mm512_add_epi64(a1,v8),a3=_mm512_add_epi64(a2,v8);
  const __m512i p0=_mm512_mullo_epi64(_mm512_xor_si512(vD,a0),vI),p1=_mm512_mullo_epi64(_mm512_xor_si512(vD,a1),vI);
  const __m512i p2=_mm512_mullo_epi64(_mm512_xor_si512(vD,a2),vI),p3=_mm512_mullo_epi64(_mm512_xor_si512(vD,a3),vI);
  a0=_mm512_add_epi64(a3,v8);
  // one compare for the 32: the smallest of the four products per lane (the four masks are only made when one passes)
  if(__builtin_expect(_mm512_cmplt_epu64_mask(_mm512_min_epu64(_mm512_min_epu64(p0,p1),_mm512_min_epu64(p2,p3)),vL)==0,1))continue; // sample 1 off in all 32
  const uint32_t m0=_mm512_cmplt_epu64_mask(p0,vL),m1=_mm512_cmplt_epu64_mask(p1,vL),m2=_mm512_cmplt_epu64_mask(p2,vL),m3=_mm512_cmplt_epu64_mask(p3,vL);
  const uint64_t hit=ks_hits(um,n,limit,A,INC,m0|(m1<<8)|(m2<<16)|(m3<<24));
  if(hit!=UINT64_MAX)return c+hit;
 }
 for(;c<e;++c,A+=INC){ // the tail of the block, as the scalar loop
  if(__builtin_expect(((D1^A)*I1)>=LIM,1))continue;
  if(ks_full(um,n,limit,A))return c;
 }
 return UINT64_MAX;
}
static bool find_key(const uint64_t* um,size_t n,uint64_t limit,uint64_t step,uint64_t& key,int T){
 if(n<6||limit<step)return false;
 const uint64_t cand=limit/step;
 const uint64_t inc=(M1*step)&MASK;
 // The search costs one candidate per counter below sample 0's own, so the test per candidate is what
 // counts. First round: sample 1 has to agree as well (one multiplication per candidate; a wrong key
 // passes it once in limit / 2^40 tries). Only if that finds nothing - sample 1 is the odd one - the
 // second round runs with the full rule (samples 1 and 2 both off: skip). Any key that passes the full
 // rule is a valid key; which one is found only matters for speed.
 for(int round=0;round<2;++round){
  atomic<uint64_t> best{UINT64_MAX};
  const bool ks5=ks_has512();
  #pragma omp parallel num_threads(T)
  {
   const int t=omp_get_thread_num();
   const uint64_t BL=1u<<16; // blocks are dealt round-robin so every thread stays near the low counters
   const uint64_t u0=um[0],u1=um[1],u2=um[2];
   for(uint64_t b=(uint64_t)t*BL;b<cand&&b<best.load(memory_order_relaxed);b+=(uint64_t)T*BL){
    const uint64_t e=min(cand,b+BL);uint64_t a=(b*inc)&MASK;
    if(round==0&&limit<=MASK){
     // The first round's test with the 40-bit numbers held in the top 40 bits of a word: adding and
     // multiplying then wrap as they should without masks ((x << 24) * I1 = ((x * I1) mod 2^40) << 24).
     if(ks5){const uint64_t c=ks_block_v5(um,n,limit,inc,b,e);
      if(c!=UINT64_MAX){uint64_t cur=best.load(memory_order_relaxed);while(c<cur&&!best.compare_exchange_weak(cur,c)){}}
      continue;}
     const uint64_t D1=(u0^u1)<<24,INC=inc<<24,LIM=limit<<24;uint64_t A=a<<24;
     for(uint64_t c=b;c<e;++c,A+=INC){
      if(__builtin_expect(((D1^A)*I1)>=LIM,1))continue; // sample 1 off
      const uint64_t k=u0^(A>>24);
      size_t miss=0;for(size_t i=1;i<n&&miss<2;++i)miss+=(((um[i]^k)*I1)&MASK)>=limit;
      if(miss<2){uint64_t cur=best.load(memory_order_relaxed);while(c<cur&&!best.compare_exchange_weak(cur,c)){}break;}
     }
     continue;
    }
    for(uint64_t c=b;c<e;++c,a=(a+inc)&MASK){
     const uint64_t k=u0^a;
     if((((u1^k)*I1)&MASK)>=limit&&(round==0||(((u2^k)*I1)&MASK)>=limit))continue; // sample 1 off (second round: and sample 2)
     size_t miss=0;for(size_t i=1;i<n&&miss<2;++i)miss+=(((um[i]^k)*I1)&MASK)>=limit;
     if(miss<2){uint64_t cur=best.load(memory_order_relaxed);while(c<cur&&!best.compare_exchange_weak(cur,c)){}break;}
    }
   }
  }
  const uint64_t c=best.load();
  if(c!=UINT64_MAX){key=um[0]^((c*inc)&MASK);return true;}
 }
 return false;
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
// -DSS_CAS_FIRST (measuring option, off by default): the lock is first tried on the assumption that the slot is
// empty, without reading it; any other state falls into the same loop. Fewer instructions for a first copy, a
// second locked instruction for every later copy.
static inline __attribute__((always_inline)) void eslot_commit(ESlot& s,uint64_t kk,uint64_t uct,int64_t ingest,uint64_t spare=0){
#ifdef SS_CAS_FIRST
 uint64_t cur=0;
 if(__builtin_expect(s.k.compare_exchange_strong(cur,ESLOT_BUSY,memory_order_acquire,memory_order_relaxed),1)){
  memcpy((char*)&s+offsetof(ESlot,user),&uct,8);s.ingest=ingest;s.spare=spare;s.k.store(kk,memory_order_release);return;}
#else
 uint64_t cur=s.k.load(memory_order_relaxed);
#endif
 for(;;){
  if(__builtin_expect(cur==ESLOT_BUSY,0)){++eslotWaits;_mm_pause();cur=s.k.load(memory_order_relaxed);continue;}
  if(__builtin_expect(!s.k.compare_exchange_weak(cur,ESLOT_BUSY,memory_order_acquire,memory_order_relaxed),0)){++eslotWaits;continue;} // cur is reloaded
  if(cur==0||ingest<s.ingest){memcpy((char*)&s+offsetof(ESlot,user),&uct,8);s.ingest=ingest;s.spare=spare;s.k.store(kk,memory_order_release);}
  else s.k.store(cur,memory_order_release);
  return;
 }
}
// -DSS_ABL_CLICK=n (measuring builds only, wrong output): 1 = no click slot is written, 2 = none written and none
// asked for by the cl8 loops, 3 = written but not asked for.
static inline __attribute__((always_inline)) void cslot_commit(CSlot& s,uint64_t key,uint32_t user,uint32_t thread){
#if defined(SS_ABL_CLICK)&&(SS_ABL_CLICK==1||SS_ABL_CLICK==2)
 (void)s;(void)key;(void)user;(void)thread;return;
#endif
 const uint64_t w=(uint64_t)user|((uint64_t)(thread+1)<<32);
 __atomic_store_n(&s.key,key,__ATOMIC_RELAXED);memcpy((void*)&s.user,&w,8);
}
// SS_COMMIT=stream. A stream record into its slot, by the rule of eslot_commit: an empty slot takes it, a slot in use
// only if the record's ingest_ts is smaller (the first copy stays on equal ingest). `base` = the slot of lo 0 of the
// record's bucket, `th` = {0, owner thread of the stream << 48}, `sink` = a slot of the caller's that nobody reads:
// the record that loses is stored there, so the rule costs no branch (about 7% of the rows are later copies, in no
// order a predictor could learn). Plain stores: the caller is the only writer of the bucket's slots.
static inline __attribute__((always_inline)) void erec_to_slot(ESlot* base,const ERec& r,__m128i th,ESlot* sink){
 ESlot* const s=base+erec_lo(r);
 const bool take=(s->k.load(memory_order_relaxed)==0)|(r.ingest<s->ingest);
 ESlot* const d=take?s:sink;
 const __m128i keep=_mm_set_epi64x(0x0000FFFFFFFFFFFFll,-1); // k whole; user and cat of ucl
 const __m128i a=_mm_loadu_si128((const __m128i*)(const void*)&r),b=_mm_loadu_si128((const __m128i*)(const void*)&r+1);
 _mm_storeu_si128((__m128i*)(void*)d,_mm_or_si128(_mm_and_si128(a,keep),th));_mm_storeu_si128((__m128i*)(void*)d+1,b);
}
// All records of stream bucket b (every thread's, in thread order, each in append order) into slots: the slot of
// counter c is table[c & mask]. mask + 1 must be a multiple of the bucket's 2^bsh counters (the bucket's slots are
// contiguous in the table); denseEv with mask = ~0 gives the global array. The caller must be the only writer of
// those slots; empty slots must be zero (k == 0) beforehand. Returns the number of records read. This is THE
// commit rule of stream mode: SCATTER and the bucket stage both come through here (BUCKETS.md).
template<class TLS> static inline __attribute__((always_inline)) size_t stream_bucket_to_slots(const TLS& tls,int T,unsigned bsh,size_t b,ESlot* table,size_t mask){
 ESlot* const base=table+(((size_t)b<<bsh)&mask);ESlot sink;size_t recs=0;
 for(int t=0;t<T;++t){
  const __m128i th=_mm_set_epi64x((long long)((uint64_t)tls[t].tid<<48),0);
  if(t+1<T)tls[t+1].es.prefetch_first(b);
  tls[t].es.for_segments(b,[&](const ERec* p,size_t n){recs+=n;for(size_t i=0;i<n;++i)erec_to_slot(base,p[i],th,&sink);});}
 return recs;
}
// One dense event row into the stream of its bucket (uct without a thread: see erec_make).
static inline __attribute__((always_inline)) void estream_put(StreamStore<ERec>& es,unsigned sh,uint64_t ve,uint64_t kk,uint64_t uct,int64_t ingest,uint64_t spare){
 es.put((size_t)(ve>>sh),erec_make(kk,uct,ingest,spare,(uint32_t)ve&((1u<<sh)-1)));
}
// The eight rows of a kernel group (all ok) into their streams in one call: the same streams as eight estream_put
// calls in row order. O = the kernel's Ev8Out (ve, kk, uct, ingest, uw of rows 0-7; uct without a thread). AVX-512.
// Why: with shuffled input the row-by-row form has a branch per row ("did this record fill the line?") that is taken
// every other time in no order (half of them mispredicted), and 4 + 4 scalar moves per record. Here
//   - the records are built by transposing the kernel's columns (four unpacks, four two-source permutes) and each
//     goes to its fill line with one 32-byte store;
//   - a bucket whose line became full is only noted (pend, no branch), and the noted lines leave in one loop at the
//     end with one 64-byte load and one 64-byte non-temporal store each: one loop exit to predict per group.
// A line must not get another record between "full" and its store, so a group with two rows of one bucket (2% of
// the groups at 1,500 buckets; all of them in sorted input, where the branch per row predicts well) goes row by row.
// The record layout is that of ERec / erec_make (two records per line); nothing else here depends on it.
// The eight rows of a kernel group, row by row (plain x86-64 code; a call, to keep it out of the loops' bodies).
template<class O> [[gnu::noinline]] static void estream_put8_rows(StreamStore<ERec>& es,unsigned sh,const O& o){
 for(unsigned i=0;i<8;++i)estream_put(es,sh,o.ve[i],o.kk[i],o.uct[i],(int64_t)o.ingest[i],o.uw[i]);
}
#define SS_T5P __attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512cd,avx2,bmi,bmi2,lzcnt,popcnt")))
template<class O> SS_T5P [[gnu::noinline]] static void estream_put8_v5(StreamStore<ERec>& es,unsigned sh,const O& o){
 static_assert(StreamStore<ERec>::RPL==2);
 using BS=StreamStore<ERec>::BS;
 const __m128i shc=_mm_cvtsi32_si128((int)sh);
 const __m512i ve=_mm512_load_si512((const void*)o.ve),b8=_mm512_srl_epi64(ve,shc);
 const __mmask8 dup=_mm512_cmpeq_epi64_mask(b8,_mm512_alignr_epi64(b8,b8,1))|_mm512_cmpeq_epi64_mask(b8,_mm512_alignr_epi64(b8,b8,2))
                   |_mm512_cmpeq_epi64_mask(b8,_mm512_alignr_epi64(b8,b8,3))|_mm512_cmpeq_epi64_mask(b8,_mm512_alignr_epi64(b8,b8,4)); // every pair of rows is in one of the four rotations
 if(__builtin_expect(dup!=0,0)){estream_put8_rows(es,sh,o);return;}
 const __m512i lo48=_mm512_slli_epi64(_mm512_xor_si512(ve,_mm512_sll_epi64(b8,shc)),48);                 // the low sh bits of the counter, in bits 48-63
 const __m512i ucl=_mm512_mask_blend_epi16((__mmask32)0x88888888u,_mm512_load_si512((const void*)o.uct),lo48); // user and cat from uct, lo above them
 const __m512i kk=_mm512_load_si512((const void*)o.kk),ing=_mm512_load_si512((const void*)o.ingest),uw=_mm512_load_si512((const void*)o.uw);
 const __m512i t0=_mm512_unpacklo_epi64(kk,ucl),t1=_mm512_unpackhi_epi64(kk,ucl),t2=_mm512_unpacklo_epi64(ing,uw),t3=_mm512_unpackhi_epi64(ing,uw); // (k, ucl) and (ingest, spare) of the even / odd rows
 const __m512i ia=_mm512_setr_epi64(0,1,8,9,2,3,10,11),ib=_mm512_setr_epi64(4,5,12,13,6,7,14,15);
 const __m512i r02=_mm512_permutex2var_epi64(t0,ia,t2),r46=_mm512_permutex2var_epi64(t0,ib,t2),r13=_mm512_permutex2var_epi64(t1,ia,t3),r57=_mm512_permutex2var_epi64(t1,ib,t3); // records of rows 0|2, 4|6, 1|3, 5|7
 alignas(64) uint64_t bb[8];_mm512_store_si512((void*)bb,b8);
 ERec* const fill=es.fill;BS* const bs=es.bs;const uintptr_t cmask=es.cmask;
 uint64_t pend[8];unsigned cnt=0;
#define SS_PUT8_ROW(I,V) {const size_t b_=(size_t)bb[I];const uint32_t n_=bs[b_].n;bs[b_].n=n_+1;const unsigned pos_=n_&1u;_mm256_store_si256((__m256i*)(void*)(fill+b_*2+pos_),V);pend[cnt]=b_;cnt+=pos_;}
 SS_PUT8_ROW(0,_mm512_castsi512_si256(r02)) SS_PUT8_ROW(1,_mm512_castsi512_si256(r13)) SS_PUT8_ROW(2,_mm512_extracti64x4_epi64(r02,1)) SS_PUT8_ROW(3,_mm512_extracti64x4_epi64(r13,1))
 SS_PUT8_ROW(4,_mm512_castsi512_si256(r46)) SS_PUT8_ROW(5,_mm512_castsi512_si256(r57)) SS_PUT8_ROW(6,_mm512_extracti64x4_epi64(r46,1)) SS_PUT8_ROW(7,_mm512_extracti64x4_epi64(r57,1))
#undef SS_PUT8_ROW
 for(unsigned j=0;j<cnt;++j){const size_t b=(size_t)pend[j];BS& st=bs[b];char* w=st.w;if(__builtin_expect(!w,0))w=es.grow(b);
  _mm512_stream_si512((__m512i*)(void*)w,_mm512_load_si512((const void*)(fill+b*2)));w+=64;st.w=((uintptr_t)w&cmask)?w:nullptr;}
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
//   stream  (SS_COMMIT=stream; slot form only) the parse stage appends each dense event row to the stream of its
//           bucket (counter >> bsh; StreamStore, ERec) and writes no event slot; the SCATTER step right after the
//           parse stage fills denseEv bucket by bucket, one thread per bucket, without a lock. bwin >= bsh: slots per
//           SCATTER turn (SS_BSH itself; bsh = min(SS_BSH, 16) because a record has 16 bits for the low counter bits).
struct DenseParse{bool users=false,events=false,clicks=false,bucket=false,raw=false,twoPhase=false,batch=false,ev8=false,cl8=false,stream=false,put8=true,spf=false;unsigned ahead=8,shE=14,shC=14,bsh=14,bwin=14,bch=12;
 bool gstream=false,gspf=false,gnote=true;unsigned gbits=6;}; // gstream: general event rows go to GRec streams of 2^gbits buckets (SS_GCOMMIT=stream)
// The class and key of an event_id text (n >= 1 bytes at s, quotes stripped): the reading of the ev8g kernel (vertg).
static inline uint64_t gkey_classify(const char* s,size_t n,uint64_t& key,uint64_t& hi){
 hi=0;
 if(s[0]=='e'){
  if(n<=9){uint64_t w=0;bool g=true;for(size_t j=1;j<n;++j){const unsigned ch=(unsigned char)s[j];g&=(ch-(unsigned)'0'<10u)|(ch-(unsigned)'a'<26u);w|=(uint64_t)ch<<(8*(j-1));}
   if(g){key=w;return GCLS_SHORT;}}
  else if(n==33){uint64_t h,l;if(KeyTable::hex16(s+1,h)&&KeyTable::hex16(s+17,l)){key=l;hi=h;return GCLS_LONG;}}
 }
 key=(uint64_t)(uintptr_t)s;return GCLS_OTHER;
}
// One general event row into the calling thread's streams (h = the hash of its class, see GRec).
static inline __attribute__((always_inline)) void gstream_put(ThreadState& tl,unsigned gbits,uint64_t cls,uint64_t key,uint64_t hi,uint64_t h,uint64_t k,uint32_t user,unsigned cat,int64_t ingest){
 const size_t b=(size_t)(h>>(64-gbits));
 if(cls==GCLS_LONG)tl.gl.put(b,GRecL{key,hi,k,grec_uc(user,cat,cls),ingest,{0,0,0}});
 else tl.gs.put(b,GRec{key,k,grec_uc(user,cat,cls),ingest});
}
// The scalar entry: classify the text, hash, put.
static inline void gcommit_row(ThreadState& tl,unsigned gbits,const char* s,size_t n,uint64_t k,uint32_t user,unsigned cat,int64_t ingest){
 uint64_t key,hi;const uint64_t cls=gkey_classify(s,n,key,hi);
 gstream_put(tl,gbits,cls,key,hi,cls==GCLS_SHORT?gkey_hash_short(key):cls==GCLS_LONG?gkey_hash_long(key,hi):hkey(string_view(s,n)),k,user,cat,ingest);
}
// SS_GCOMMIT=stream while nothing reads the streams directly: one thread's streams back into its PRec buckets, so
// that the bucket dedup runs unchanged. The text of a class 0 / 1 ID is written out again ('e' + bytes + ',') into
// the thread's arena: hkey and same_event_id then see exactly the text the line had. Test bridge: costs a pass.
static void gbridge(ThreadState& tl,size_t PE,unsigned gbits){
 const size_t NB=size_t(1)<<gbits;
 char* txt=(char*)tlArena.get((size_t)tl.gs.records()*10+(size_t)tl.gl.records()*34+64); // 'e' + 8 + ',' or 'e' + 32 + ',' at most per record; hkey / same_event_id read a little past a text
 auto emit=[&](const char* id,size_t n,uint64_t k,uint64_t uc,int64_t ingest){
  const uint64_t h=hkey(string_view(id,n)),code=ingest_code(ingest);
  if(code==INGEST_SIDE)tl.ingestSide.emplace_back(id,ingest);
  tl.eb[(size_t)(((h>>40)*PE)>>24)].push_back(make_prec(id,h,code,k,(unsigned)(uint16_t)(uc>>32),(uint32_t)uc));};
 static const char* const HX="0123456789abcdef";
 for(size_t b=0;b<NB;++b){
  tl.gs.for_segments(b,[&](const GRec* p,size_t n){for(size_t i=0;i<n;++i){const GRec& r=p[i];
   if(grec_cls(r)==GCLS_SHORT){char* const t=txt;t[0]='e';memcpy(t+1,&r.key,8);size_t len=0;while(len<8&&((r.key>>(8*len))&0xff))++len;t[1+len]=',';txt+=len+2;emit(t,len+1,r.k,r.uc,r.ingest);}
   else{const char* const t=(const char*)(uintptr_t)r.key;size_t len=0;while(t[len]!=','&&t[len]!='"')++len;emit(t,len,r.k,r.uc,r.ingest);}}});
  tl.gl.for_segments(b,[&](const GRecL* p,size_t n){for(size_t i=0;i<n;++i){const GRecL& r=p[i];
   char* const t=txt;t[0]='e';for(int d=0;d<16;++d){t[1+d]=HX[(r.hi>>(60-4*d))&15];t[17+d]=HX[(r.lo>>(60-4*d))&15];}t[33]=',';txt+=34;emit(t,33,r.k,r.uc,r.ingest);}});
 }
}

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
// ==== lk8: the link kernel (text of vert/lk8.h, agent K) ====
// lk8.h - the link kernel: eight link lines per call, one per lane (agent K, 2026-11-21). Text to paste into main.cpp.
//
// A link line is `user_id_a,user_id_b`. The links stage (DENSE.md, stage 2) calls a line SIMPLE if both IDs are dense
// user IDs (canonical 'u' + base 36, user index below limI) with aliases 0-7 of the same user index. lk8 finds the
// simple lines; every other line is refused and stays with the line code of main.cpp (link_split + link_dense), unchanged.
//
// CONTRACT  lk8_v5(lp, klim, c, o) / lk8_ref (the scalar twin: same contract, plain code, same outputs bit for bit)
//   lp[0..7]    line starts, one per lane. lp[i] must be a true line start with at least 96 readable bytes.
//   klim[0..7]  a lane with lp[i] >= klim[i] is dead: nothing is reported for it and lp[i] stays (it is still read).
//   c           ku = the user key (dense.ku), limU = min(limI, 2^31) << 9 (a counter below it has an index below limI).
//   o.ok        bit i: lane i is live and its line is a simple line as link_split + link_dense see it: exactly two fields,
//               each `u` + 1-8 canonical base-36 digits below 2^40 (bare, or the whole field wrapped in quotes), both user
//               indexes below limI and equal, both aliases 0-7. Then o.idx[i] = the index, o.aa[i] / o.ab[i] = the two
//               aliases, and lp[i] has been moved to the start of the next line (the byte after the line's newline).
//   o.un        the ok lanes with aa != ab (the lines that are a union).
//   o.rec[i]    for a union lane: idx << 5 | pair number of the two aliases, lk_pair(lo, hi) = hi*(hi-1)/2 + lo (0..27;
//               LK_PAIR_LO / LK_PAIR_HI turn it back). It fits 32 bits when idx < 2^27. Unspecified for other lanes.
//   o.bk[i]     for a union lane: (idx >> LK_BLOCK_BITS) & c.bmask, the bucket of the index; c.btrash for every other lane.
//   o.slow      bit i: lane i is live and the line was refused: lp[i] is unchanged, the caller's line code decides
//               everything about the line, including where the next one starts.
//   o.idx[i]    0 for every lane that is not ok (so it can be used as a prefetch address without a test).
//   o.mx[0..7]  running maximum of the indexes of ok lines, per lane; the caller zeroes it before the first call.
// Why accept => same fields as link_split: P = first comma and L = first newline of the 64 bytes at lp[i]. Field a is
// bytes [0,P), field b is bytes (P,L). Every byte of both is checked: an optional opening quote, the letter, N digits
// (N = field length - 1 - 2*quoted), and for a quoted field the closing quote as its last byte. So the line has one
// comma, quotes only around whole fields, both fields non-empty, and L <= 23: link_split's two-compare path returns
// these two fields and next = lp[i]+L+1. (A newline before the comma, a missing comma, a second comma, a quote in
// the wrong place, an empty field, \r, upper case: some checked byte is not a base-36 digit, or a length is out of range.)
// Reads: 64 bytes at lp[i], 16 bytes at lp[i]+P+1 with P <= 64, i.e. below lp[i]+82. No read before lp[i].
// CPU: AVX-512 F+BW+DQ+VL+CD, BMI1 (lk_has512). No gather instructions. Real calls, not always_inline.
#ifndef LK8_H
#define LK8_H
namespace vlk{
#define VLK_T5 __attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512cd,avx2,bmi,bmi2,lzcnt,popcnt")))
static constexpr uint64_t MASK40=(1ull<<40)-1;
static constexpr uint64_t M1=0x9E3779B97F4Bull&MASK40,M2=0xD6E8FEB86659ull&MASK40;
static constexpr uint64_t inv40(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&MASK40;}
static constexpr uint64_t I1=inv40(M1),I2=inv40(M2);
static constexpr unsigned LK_GEN_ALIAS_BITS=9,LK_ALIAS_BITS=3; // main.cpp: dn::GEN_ALIAS_BITS, dn::ALIAS_BITS
static constexpr unsigned LK_BLOCK_BITS=12; // 4096 user indexes (16 KB of component words) per block; blocks go to buckets in turn
struct LkConst{uint64_t ku,limU,bmask,btrash;}; // bmask = buckets - 1 (a power of two), btrash = the bucket number for lanes without a union
static inline LkConst lk_const(uint64_t ku,uint64_t limI,uint64_t buckets){
 return {ku,(limI<(1ull<<(40-LK_GEN_ALIAS_BITS))?limI:(1ull<<(40-LK_GEN_ALIAS_BITS)))<<LK_GEN_ALIAS_BITS,buckets-1,buckets};
}
static constexpr uint64_t lk_pair(uint64_t lo,uint64_t hi){return hi*(hi-1)/2+lo;}
static constexpr uint8_t LK_PAIR_LO[28]={0, 0,1, 0,1,2, 0,1,2,3, 0,1,2,3,4, 0,1,2,3,4,5, 0,1,2,3,4,5,6};
static constexpr uint8_t LK_PAIR_HI[28]={1, 2,2, 3,3,3, 4,4,4,4, 5,5,5,5,5, 6,6,6,6,6,6, 7,7,7,7,7,7,7};
struct alignas(64) LkOut{uint64_t idx[8],aa[8],ab[8],mx[8],rec[8],bk[8];uint8_t ok,un,slow;};
static inline bool lk_has512(){
 static const bool h=__builtin_cpu_supports("avx512f")&&__builtin_cpu_supports("avx512bw")&&__builtin_cpu_supports("avx512dq")&&__builtin_cpu_supports("avx512vl")&&__builtin_cpu_supports("avx512cd")&&__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("lzcnt")&&__builtin_cpu_supports("popcnt");
 return h;
}
// ---- the scalar twin (the definition; also what runs under SS_LINKS=ref and in valgrind) ----
// One field of f bytes at s (16 readable): true if it is `u` + canonical base 36 below 2^40, bare or quoted; ctr = its counter.
static inline bool lk_field_ref(const unsigned char* s,uint64_t f,uint64_t key,uint64_t& ctr){
 if(f<2||f>11)return false;
 const unsigned z=s[0]=='"';
 if(z&&(f<4||s[f-1]!='"'))return false;
 if(s[z]!='u')return false;
 const uint64_t n=f-1-2*z; // digits
 if(n<1||n>8)return false;
 const unsigned char* d=s+z+1;
 if(d[0]=='0'&&n>1)return false;
 uint64_t v=0;
 for(uint64_t i=0;i<n;++i){unsigned x=(unsigned)d[i]-(unsigned)'0';if(x>9){x=(unsigned)d[i]-(unsigned)'a';if(x>25)return false;x+=10;}v=v*36+x;}
 if(v>MASK40)return false;
 uint64_t m=(v*I2)&MASK40;m^=(m>>17)^(m>>34);
 ctr=((m^key)*I1)&MASK40;return true;
}
static inline void lk8_ref(const char** lp,const char* const* klim,const LkConst& c,LkOut& o){
 unsigned ok=0,un=0,slow=0;
 for(int i=0;i<8;++i){
  o.idx[i]=0;o.aa[i]=0;o.ab[i]=0;o.rec[i]=0;o.bk[i]=c.btrash;
  const unsigned char* p=(const unsigned char*)lp[i];
  if((const char*)p>=klim[i])continue;
  uint64_t P=64,L=64;
  for(uint64_t k=0;k<64;++k)if(p[k]==','){P=k;break;}
  for(uint64_t k=0;k<64;++k)if(p[k]=='\n'){L=k;break;}
  uint64_t ca=0,cb=0;
  const bool good=P<L&&lk_field_ref(p,P,c.ku,ca)&&lk_field_ref(p+P+1,L-P-1,c.ku,cb)&&ca<c.limU&&cb<c.limU
   &&(ca>>LK_GEN_ALIAS_BITS)==(cb>>LK_GEN_ALIAS_BITS)&&(ca&((1u<<LK_GEN_ALIAS_BITS)-1))<(1u<<LK_ALIAS_BITS)&&(cb&((1u<<LK_GEN_ALIAS_BITS)-1))<(1u<<LK_ALIAS_BITS);
  if(!good){slow|=1u<<i;continue;}
  ok|=1u<<i;
  o.idx[i]=ca>>LK_GEN_ALIAS_BITS;o.aa[i]=ca&((1u<<LK_ALIAS_BITS)-1);o.ab[i]=cb&((1u<<LK_ALIAS_BITS)-1);
  if(o.aa[i]!=o.ab[i]){un|=1u<<i;
   const uint64_t lo=o.aa[i]<o.ab[i]?o.aa[i]:o.ab[i],hi=o.aa[i]<o.ab[i]?o.ab[i]:o.aa[i];
   o.rec[i]=o.idx[i]<<5|lk_pair(lo,hi);o.bk[i]=(o.idx[i]>>LK_BLOCK_BITS)&c.bmask;}
  if(o.idx[i]>o.mx[i])o.mx[i]=o.idx[i];
  lp[i]=(const char*)p+L+1;
 }
 o.ok=(uint8_t)ok;o.un=(uint8_t)un;o.slow=(uint8_t)slow;
}
// ---- the AVX-512 kernel ----
// constants, one 8-byte entry each, broadcast from memory (read through a pointer the compiler cannot see through, so
// that it does not rebuild them in registers on every use)
static const uint64_t LKK[]={
 /*0*/ 64,
 /*1*/ 0x3030303030303030ull,           // eight '0'
 /*2*/ 0x3030303030303030ull,           // '0' per byte
 /*3*/ 0x6161616161616161ull,           // 'a' per byte
 /*4*/ 0x0909090909090909ull,
 /*5*/ 0x1919191919191919ull,           // 25
 /*6*/ 0xA9A9A9A9A9A9A9A9ull,           // 10-'a'
 /*7*/ 0x0124012401240124ull,           // bytes 36,1
 /*8*/ 0x0001051000010510ull,           // words 1296,1
 /*9*/ 1679616ull,                      // 36^4
 /*10*/ MASK40,
 /*11*/ I2,
 /*12*/ I1,
 /*13*/ 0xFFull,
 /*14*/ 0xFFFFull,
 /*15*/ 0x75ull,                        // 'u'
 /*16*/ 0x7522ull,                      // '"' then 'u'
 /*17*/ 0x22ull,                        // '"'
 /*18*/ 8,
 /*19*/ 16,
 /*20*/ 1,
 /*21*/ 3,
 /*22*/ ~7ull,                          // counter bits that must agree: everything above the low three alias bits
 /*23*/ (uint64_t)(((1u<<LK_GEN_ALIAS_BITS)-1)&~((1u<<LK_ALIAS_BITS)-1)), // alias bits that must be zero (alias < 8)
 /*24*/ 7,
};
alignas(64) static const uint64_t LKV[]={
 /*0*/ 78364164096ull,0,36,1296,46656,1679616,60466176,2176782336ull, // MIN36[n] by the low three bits of n
 /*8*/ 0,2,4,6,8,10,12,14,
 /*16*/ 1,3,5,7,9,11,13,15,
 /*24*/ 0,0,1,3,6,10,15,21,                                           // hi*(hi-1)/2
};
static inline __attribute__((always_inline)) const uint64_t* lkk_ptr(){const uint64_t* p=LKK;__asm__("":"+r"(p));return p;}
static inline __attribute__((always_inline)) const uint64_t* lkv_ptr(){const uint64_t* p=LKV;__asm__("":"+r"(p));return p;}
#define VLK_K(i) _mm512_set1_epi64((long long)K[i])
// One field per lane: lo / hi = its first 16 bytes, F = its length. Returns the counter; ok = `u` + canonical base 36
// below 2^40 (bare or quoted) with a counter below limU.
VLK_T5 static inline __attribute__((always_inline)) __m512i lk_field_v5(__m512i lo,__m512i hi,__m512i F,const uint64_t* K,const uint64_t* V,const LkConst& c,__mmask8& okOut){
 const __m512i c64=VLK_K(0),c8=VLK_K(18),ff=VLK_K(13);
 const __mmask8 kq=_mm512_cmpeq_epi64_mask(_mm512_and_si512(lo,VLK_K(14)),VLK_K(16)); // starts with `"u`
 const __mmask8 kb=_mm512_cmpeq_epi64_mask(_mm512_and_si512(lo,ff),VLK_K(15));        // starts with `u`
 const __m512i s=_mm512_mask_mov_epi64(c8,kq,VLK_K(19));                               // bits to the first digit
 const __m512i W=_mm512_or_si512(_mm512_srlv_epi64(lo,s),_mm512_sllv_epi64(hi,_mm512_sub_epi64(c64,s))); // the 8 bytes from the first digit
 const __m512i N=_mm512_sub_epi64(F,_mm512_mask_mov_epi64(VLK_K(20),kq,VLK_K(21)));   // digits = F - 1 - 2*quoted
 // closing quote of a quoted field: byte F-1 of the sixteen (a shift count of 64 or more gives zero)
 const __m512i t=_mm512_sub_epi64(_mm512_slli_epi64(F,3),c8);
 const __m512i cq=_mm512_or_si512(_mm512_srlv_epi64(lo,t),_mm512_srlv_epi64(hi,_mm512_sub_epi64(t,c64)));
 const __mmask8 kc=_mm512_mask_cmpeq_epi64_mask(kq,_mm512_and_si512(cq,ff),VLK_K(17));
 const __mmask8 shape=_kor_mask8(kb,kc);
 // the digits: moved so that the last one is the top byte, '0' below the first; n = 0 gives val = 0 < MIN36[8] (the entry
 // n & 7 == 0 picks), n >= 9 or negative gives x = 0, and a zero byte is not a digit
 const __m512i n8=_mm512_slli_epi64(N,3);
 const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(W,_mm512_sub_epi64(c64,n8)),_mm512_srlv_epi64(VLK_K(1),n8));
 const __m512i tt=_mm512_sub_epi8(x,VLK_K(2)),u=_mm512_sub_epi8(x,VLK_K(3));
 const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(tt,VLK_K(4)),_mm512_subs_epu8(u,VLK_K(5)));
 const __m512i d=_mm512_min_epu8(tt,_mm512_add_epi8(x,VLK_K(6)));
 const __m512i p2=_mm512_maddubs_epi16(d,VLK_K(7));
 const __m512i p4=_mm512_madd_epi16(p2,VLK_K(8));
 const __m512i val=_mm512_add_epi64(_mm512_mul_epu32(p4,VLK_K(9)),_mm512_srli_epi64(p4,32));
 const __m512i min36=_mm512_permutexvar_epi64(N,_mm512_load_si512((const void*)V));
 const __m512i mask=VLK_K(10);
 __mmask8 ok=_mm512_mask_testn_epi64_mask(shape,bad,bad);
 ok=_mm512_mask_cmpge_epu64_mask(ok,val,min36);
 ok=_mm512_mask_cmple_epu64_mask(ok,val,mask);
 __m512i m=_mm512_and_si512(_mm512_mullo_epi64(val,VLK_K(11)),mask);
 m=_mm512_ternarylogic_epi64(m,_mm512_srli_epi64(m,17),_mm512_srli_epi64(m,34),0x96);
 m=_mm512_xor_si512(m,_mm512_set1_epi64((long long)c.ku));
 const __m512i ctr=_mm512_and_si512(_mm512_mullo_epi64(m,VLK_K(12)),mask);
 okOut=_mm512_mask_cmplt_epu64_mask(ok,ctr,_mm512_set1_epi64((long long)c.limU));
 return ctr;
}
VLK_T5 [[gnu::noinline]] static void lk8_v5(const char** lp,const char* const* klim,const LkConst& c,LkOut& o){
 const uint64_t* const K=lkk_ptr();const uint64_t* const V=lkv_ptr();
 alignas(64) uint64_t PL[16]; // P of the eight lanes, then L
 alignas(64) uint64_t AB[32]; // the first 16 bytes of field a of each lane, then of field b
 const __m512i vnl=_mm512_set1_epi8('\n'),vc=_mm512_set1_epi8(',');
 for(int i=0;i<8;++i){
  const char* const p=lp[i];
  const __m512i x=_mm512_loadu_si512((const void*)p);
  const uint64_t L=_tzcnt_u64(_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vnl))),P=_tzcnt_u64(_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc)));
  PL[i]=P;PL[8+i]=L;
  _mm_store_si128((__m128i*)(AB+2*i),_mm512_castsi512_si128(x));
  _mm_store_si128((__m128i*)(AB+16+2*i),_mm_loadu_si128((const __m128i*)(p+P+1)));
 }
 const uint64_t* pl=PL;const uint64_t* ab=AB;__asm__("":"+r"(pl),"+r"(ab)::"memory"); // the vector part reads what the loop stored
 const __m512i Pv=_mm512_load_si512((const void*)pl),Lv=_mm512_load_si512((const void*)(pl+8));
 const __m512i ie=_mm512_load_si512((const void*)(V+8)),io=_mm512_load_si512((const void*)(V+16));
 const __m512i Z0=_mm512_load_si512((const void*)ab),Z1=_mm512_load_si512((const void*)(ab+8));
 const __m512i Z2=_mm512_load_si512((const void*)(ab+16)),Z3=_mm512_load_si512((const void*)(ab+24));
 const __m512i Alo=_mm512_permutex2var_epi64(Z0,ie,Z1),Ahi=_mm512_permutex2var_epi64(Z0,io,Z1);
 const __m512i Blo=_mm512_permutex2var_epi64(Z2,ie,Z3),Bhi=_mm512_permutex2var_epi64(Z2,io,Z3);
 const __m512i one=VLK_K(20);
 __mmask8 oka,okb;
 const __m512i ca=lk_field_v5(Alo,Ahi,Pv,K,V,c,oka);
 const __m512i cb=lk_field_v5(Blo,Bhi,_mm512_sub_epi64(_mm512_sub_epi64(Lv,Pv),one),K,V,c,okb);
 const __m512i LP=_mm512_loadu_si512((const void*)lp);
 __mmask8 ok=_mm512_mask_cmplt_epu64_mask(_kand_mask8(oka,okb),LP,_mm512_loadu_si512((const void*)klim));
 const __mmask8 live=_mm512_cmplt_epu64_mask(LP,_mm512_loadu_si512((const void*)klim));
 ok=_mm512_mask_testn_epi64_mask(ok,_mm512_xor_si512(ca,cb),VLK_K(22)); // same index (and then alias b < 8 if alias a is)
 ok=_mm512_mask_testn_epi64_mask(ok,ca,VLK_K(23));                       // alias a < 8
 const __m512i idx=_mm512_maskz_srli_epi64(ok,ca,LK_GEN_ALIAS_BITS);
 const __m512i a7=VLK_K(24),aa=_mm512_and_si512(ca,a7),ab7=_mm512_and_si512(cb,a7);
 _mm512_store_si512((void*)o.idx,idx);
 _mm512_store_si512((void*)o.aa,aa);
 _mm512_store_si512((void*)o.ab,ab7);
 _mm512_store_si512((void*)o.mx,_mm512_max_epu64(_mm512_load_si512((const void*)o.mx),idx));
 _mm512_mask_storeu_epi64((void*)lp,ok,_mm512_add_epi64(_mm512_add_epi64(LP,Lv),one));
 const __mmask8 un=_mm512_mask_cmpneq_epi64_mask(ok,aa,ab7);
 const __m512i tri=_mm512_permutexvar_epi64(_mm512_max_epu64(aa,ab7),_mm512_load_si512((const void*)(V+24)));
 _mm512_store_si512((void*)o.rec,_mm512_or_si512(_mm512_slli_epi64(idx,5),_mm512_add_epi64(tri,_mm512_min_epu64(aa,ab7))));
 _mm512_store_si512((void*)o.bk,_mm512_mask_and_epi64(_mm512_set1_epi64((long long)c.btrash),un,_mm512_srli_epi64(idx,LK_BLOCK_BITS),_mm512_set1_epi64((long long)c.bmask)));
 o.ok=(uint8_t)_cvtmask8_u32(ok);
 o.un=(uint8_t)_cvtmask8_u32(un);
 o.slow=(uint8_t)_cvtmask8_u32(_kandn_mask8(ok,live));
}
#undef VLK_K
// ---- the unions of one bucket (second step) ----
// comp = the alias words (main.cpp: denseComp; see comp_union there for the format: three bits per alias = alias XOR the
// smallest alias of its group). Each record (LkOut::rec) is one union. The caller is the only thread that changes the
// words these records name while the call runs.
static constexpr uint32_t LK_IDENT=0xFAC688u; // main.cpp: COMP_IDENT
static inline __attribute__((always_inline)) void lk_union1(uint32_t* comp,uint32_t rec){ // the arithmetic of comp_union, plain load and store
 const uint32_t a=LK_PAIR_LO[rec&31],b=LK_PAIR_HI[rec&31];uint32_t& c=comp[rec>>5];
 const uint32_t w=c^LK_IDENT,ra=(w>>(3*a))&7,rb=(w>>(3*b))&7;
 if(ra==rb)return;
 const uint32_t lo=ra<rb?ra:rb,hi=ra<rb?rb:ra,F=0x249249u,x=w^(hi*F),eq=~(x|(x>>1)|(x>>2))&F;
 c=(w^((hi^lo)*eq))^LK_IDENT;
}
static inline void lk_union_ref(uint32_t* comp,const uint32_t* b,const uint32_t* e){for(;b<e;++b)lk_union1(comp,*b);}
alignas(64) static const uint32_t LKU[]={
 /*0*/  0,0,3,0,3,6,0,3,6,9,0,3,6,9,12,0,   3,6,9,12,15,0,3,6,9,12,15,18,0,0,0,0,                 // 3 * LK_PAIR_LO[t]
 /*32*/ 3,6,6,9,9,9,12,12,12,12,15,15,15,15,15,18,   18,18,18,18,18,21,21,21,21,21,21,21,0,0,0,0, // 3 * LK_PAIR_HI[t]
 /*64*/ 0,0x249249u,2*0x249249u,3*0x249249u,4*0x249249u,5*0x249249u,6*0x249249u,7*0x249249u,0,0,0,0,0,0,0,0, // root * F
};
// Sixteen records per turn: the sixteen words are read one by one (no gather instruction), changed together and written
// back one by one. Two records of one turn that name the same index would not see each other's result, so a turn with
// a repeated index (vpconflictd) is done one record at a time, like the last n mod 16 records. A record whose two
// aliases are already in one group writes its word back unchanged.
VLK_T5 [[gnu::noinline]] static void lk_union_v5(uint32_t* comp,const uint32_t* b,const uint32_t* e){
 const uint32_t* U=LKU;__asm__("":"+r"(U));
 const __m512i salo=_mm512_load_si512((const void*)U),sahi=_mm512_load_si512((const void*)(U+16));
 const __m512i sblo=_mm512_load_si512((const void*)(U+32)),sbhi=_mm512_load_si512((const void*)(U+48));
 const __m512i hf=_mm512_load_si512((const void*)(U+64));
 const __m512i ident=_mm512_set1_epi32((int)LK_IDENT),F=_mm512_set1_epi32(0x249249),c31=_mm512_set1_epi32(31),c7=_mm512_set1_epi32(7);
 for(;e-b>=16;b+=16){
  const __m512i R=_mm512_loadu_si512((const void*)b),idx=_mm512_srli_epi32(R,5);
  const __m512i cf=_mm512_conflict_epi32(idx);
  if(__builtin_expect(_mm512_test_epi32_mask(cf,cf)!=0,0)){for(int i=0;i<16;++i)lk_union1(comp,b[i]);continue;}
  alignas(64) uint32_t IX[16],W[16];
  _mm512_store_si512((void*)IX,idx);
  {const uint32_t* ix=IX;__asm__("":"+r"(ix)::"memory");
   for(int i=0;i<16;++i){uint32_t v=comp[ix[i]];__asm__("":"+r"(v));W[i]=v;}} // (the empty asm keeps these loads scalar)
  const uint32_t* wp=W;__asm__("":"+r"(wp)::"memory");
  const __m512i w=_mm512_xor_si512(_mm512_load_si512((const void*)wp),ident);
  const __m512i t=_mm512_and_si512(R,c31);
  const __m512i ra=_mm512_and_si512(_mm512_srlv_epi32(w,_mm512_permutex2var_epi32(salo,t,sahi)),c7);
  const __m512i rb=_mm512_and_si512(_mm512_srlv_epi32(w,_mm512_permutex2var_epi32(sblo,t,sbhi)),c7);
  const __m512i lo=_mm512_min_epu32(ra,rb),hi=_mm512_max_epu32(ra,rb);
  const __m512i x=_mm512_xor_si512(w,_mm512_permutexvar_epi32(hi,hf));
  const __m512i eq=_mm512_and_si512(_mm512_ternarylogic_epi32(x,_mm512_srli_epi32(x,1),_mm512_srli_epi32(x,2),0x01),F); // fields equal to hi
  const __m512i w2=_mm512_xor_si512(w,_mm512_mullo_epi32(eq,_mm512_xor_si512(hi,lo)));
  alignas(64) uint32_t O[16];
  _mm512_store_si512((void*)O,_mm512_xor_si512(w2,ident));
  {const uint32_t* ix=IX;const uint32_t* op=O;__asm__("":"+r"(ix),"+r"(op)::"memory");
   for(int i=0;i<16;++i){uint32_t v=op[i];__asm__("":"+r"(v));comp[ix[i]]=v;}}
 }
 for(;b<e;++b)lk_union1(comp,*b);
}
} // namespace vlk
#endif
static_assert(vlk::MASK40==dn::MASK&&vlk::I1==dn::I1&&vlk::I2==dn::I2&&vlk::LK_GEN_ALIAS_BITS==dn::GEN_ALIAS_BITS&&vlk::LK_ALIAS_BITS==dn::ALIAS_BITS&&vlk::LK_IDENT==COMP_IDENT);
// ==== end of lk8 ====
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
// A user ID that does not pack (over 11 bytes: every canonical long ID): through the table right here, as the
// general handler's user_of does. Nothing waits on it, so it can be done in the middle of a pending batch.
static inline uint32_t long_user(const char* u,size_t n,KeyTable& tab,ThreadState& tl){
 const string_view t(u,n);const uint32_t g=tab.find_or_add(t);return g!=KeyTable::NONE?g:(LOCAL_USER|tl.users.intern(t));
}
// A waiting record whose user ID does not pack (click_line_v3, DREC=false): k1 = the length of the text (below 2^24,
// so no packed k1 looks like it), k0 = KeyTable::long_key's hash, j = its slot (asked for when the line was read).
static inline void pend_long_user(PendUser& pu,const char* u,size_t n,KeyTable& tab,uint32_t* up){
 KeyTable::Long key;const uint64_t h=KeyTable::long_key(string_view(u,n),key);
 const size_t j=h&tab.mask;__builtin_prefetch(&tab.slots[j],0,1);
 pu.k0=h;pu.u=u;pu.up=up;pu.j=(uint32_t)j;pu.k1=(uint32_t)n;
}
static void resolve_users(const PendUser* pend,const PendUser* pendEnd,KeyTable& tab,ThreadState& tl){
 const auto R_=memory_order_relaxed;
 KeyTable::Slot* const slots=tab.slots;
 for(;pend!=pendEnd;++pend){
  const PendUser& q=*pend;KeyTable::Slot& e=slots[q.j];uint32_t g;
  if(__builtin_expect(e.k1.load(memory_order_acquire)==q.k1&&e.k0.load(R_)==q.k0,1))g=e.v.load(R_);
  else if(!(q.k1>>24)){ // a text that does not pack (pend_long_user)
   const string_view u(q.u,(size_t)q.k1);KeyTable::Long key;const uint64_t h=KeyTable::long_key(u,key);
   g=tab.find_or_add_long_key(key,h,u);
   if(g==KeyTable::NONE)g=LOCAL_USER|tl.users.intern(u);
  }
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
 bool dUsers;uint64_t ke,ku,limE;ESlot* ev;uint64_t tid48;bool bucket,raw;unsigned ahead,shE;uint32_t loMask;Bucket<DRec>* db; // dense route (dparse); tid48 = thread index << 48 (eslot_uct), 0 in stream mode (a record has the low counter bits there)
 bool gstrm,gspf,gnote;unsigned gsh;StreamStore<GRec>* gs;StreamStore<GRecL>* gl; /* general route, SS_GCOMMIT=stream: rows go to gs / gl (bucket = hash >> gsh) */
 bool strm,strm8,spf;unsigned bsh;StreamStore<ERec>* es; // SS_COMMIT=stream: rows go to es (bucket = counter >> bsh), no event slot is written or asked for; strm8: whole kernel groups through estream_put8_v5
 EvLoop(ThreadState& tl_,KeyTable& tab_,const KeyTable& products_,size_t PE_,const char* end_):tl(tl_),tab(tab_),products(products_),PE(PE_),end(end_),memo(date_memo3()),
  wstart(window_start),wlen((uint64_t)(window_end-window_start)),ibase((uint64_t)ingestBase),buckets(tl_.eb.data()),slots(tab_.slots),tmask(tab_.mask),
  dUsers(dparse.users),ke(dense.ke),ku(dense.ku),limE(dense.limE),ev(denseEv),tid48(dparse.stream?0:(uint64_t)tl_.tid<<48),bucket(dparse.bucket),raw(dparse.raw),ahead(dparse.ahead),shE(dparse.shE),loMask((1u<<dparse.shE)-1),db(tl_.db.data()),
  gstrm(dparse.gstream),gspf(dparse.gstream&&dparse.gspf),gnote(dparse.gstream&&dparse.gnote),gsh(64-dparse.gbits),gs(&tl_.gs),gl(&tl_.gl),
  strm(dparse.stream),strm8(dparse.stream&&dparse.put8&&vlk::lk_has512()),spf(dparse.stream&&dparse.spf),bsh(dparse.bsh),es(&tl_.es){}
};
// A finished dense event row of a line loop: into its slot, or (stream mode) behind the stream of its bucket.
static inline __attribute__((always_inline)) void eslot_put(const EvLoop& lp,uint64_t ve,uint64_t kk,uint64_t uct,int64_t ingest,uint64_t spare){
 if(lp.strm)estream_put(*lp.es,lp.bsh,ve,kk,uct,ingest,spare);else eslot_commit(lp.ev[ve],kk,uct,ingest,spare);
}
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
    if(__builtin_expect(s0[0]=='e'&&parse36_v3(s0+1,l0-1,ye)&&(ve=dn::index_of(ye,lp.ke))<lp.limE,1)){dEv=true;if(!lp.strm)__builtin_prefetch(&lp.ev[ve],1,3);}
   }else if(DREC){
    uint64_t ye,yu;unsigned me,mu;
    if(__builtin_expect(parse36x2_v3(s0+1,l0-1,s1+1,l1-1,ye,yu,me,mu),1)){
     if(__builtin_expect(s0[0]=='e'&&parse36x2_ok(ye,me)&&(ve=dn::index_of(ye,lp.ke))<lp.limE,1)){dEv=true;if(!lp.bucket&&!lp.strm)__builtin_prefetch(&lp.ev[ve],1,3);}
     uDense=s1[0]=='u'&&parse36x2_ok(yu,mu)&&dense_user_code(dn::index_of(yu,lp.ku),ucode);
    }else{ // a byte that is no digit in one of the two texts: each on its own
     if(s0[0]=='e'&&parse36_v3(s0+1,l0-1,ye)&&(ve=dn::index_of(ye,lp.ke))<lp.limE){dEv=true;if(!lp.bucket&&!lp.strm)__builtin_prefetch(&lp.ev[ve],1,3);}
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
     if(__builtin_expect(!KeyTable::pack(string_view(s1,l1),pu.k0,pu.k1),0))user=long_user(s1,l1,tab,tl);
     else{pu.j=(uint32_t)(KeyTable::hash_packed(pu.k0,pu.k1)&tmask);pu.u=s1;pu.up=&user;
      resolve_users(&pu,&pu+1,tab,tl);}
    }
    if(__builtin_expect(dEv,1)){
     if(PRE){send=max(send,(uint32_t)ve+1);eslot_put(lp,ve,k?k:ESLOT_INVALID,(uint64_t)user|((uint64_t)cat<<32)|lp.tid48,ingest,rawU);}
     else if(lp.bucket){ // bucket form: appended by counter range, like a PRec
      Bucket<DRec>& vec=lp.db[ve>>lp.shE];
      if(__builtin_expect(vec.full(),0))vec.reserve(max<size_t>(16,2*vec.capacity()));
      DRec r;r.k=k;r.ingest=ingest;r.user=user;r.cat=(uint16_t)cat;r.lo=(uint16_t)((uint32_t)ve&lp.loMask);
      vec.append(r);
     }else{
      send=max(send,(uint32_t)ve+1);
      const uint64_t kk=k?k:ESLOT_INVALID,uct=(uint64_t)user|((uint64_t)cat<<32)|lp.tid48; // cat < 2^16
      if(lp.strm)estream_put(*lp.es,lp.bsh,ve,kk,uct,ingest,rawU); // stream mode: no slot, so nothing to wait for
      else if(lp.ahead){ // the row waits; the row it replaces has had `ahead` rows of time for its slot's memory to arrive
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
    if(tableUser&&__builtin_expect(!KeyTable::pack(string_view(s1,l1),k0,k1),0)){ucode=long_user(s1,l1,tab,tl);tableUser=false;} // long user ID: its number right here
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
// ---- Raw users read where they lie (the folded form: no pass that rewrites the slots). ----
// What a raw text stands for without asking the ID table: its DENSE_USER code, or the raw word itself
// (DENSE_USER_RAW | length) if the text is not a dense user's. Exactly the test of SS_RESOLVE_RAW.
SS_V3 [[gnu::noinline]] static uint32_t raw_dense_code_v3(uint64_t raw,uint32_t len){
 uint64_t y;uint32_t code=0;
 return parse36_v3((const char*)&raw,len,y)&&dense_user_code(dn::index_of(y,dense.ku),code)?code:(DENSE_USER_RAW|len);
}
[[gnu::noinline]] static uint32_t raw_dense_code_plain(uint64_t raw,uint32_t len){
 uint64_t y;uint32_t code=0;
 return dn::parse36((const char*)&raw,len,y)&&dense_user_code(dn::index_of(y,dense.ku),code)?code:(DENSE_USER_RAW|len);
}
// A user word that is a DENSE_USER code (top bits 01; a raw word has 11, a LOCAL number 10, a table number 00).
static inline __attribute__((always_inline)) bool is_dense_code(uint32_t u){return (u>>30)==1u;}
// The first 16 bytes of an event slot: all a reader of resolved slots needs (see ev_at in run()).
struct ESmall{uint64_t k;uint32_t user;uint16_t cat;uint16_t thread;};
static_assert(sizeof(ESmall)==16&&offsetof(ESmall,user)==offsetof(ESlot,user)&&offsetof(ESmall,cat)==offsetof(ESlot,cat)&&offsetof(ESmall,thread)==offsetof(ESlot,thread)&&offsetof(ESlot,k)==0);
// One thread's reader of slot users when the slots are left as the parser wrote them. code() gives the
// user word of a slot holding a valid event: unchanged if the line resolved it (DENSE_USER code, table
// number, LOCAL number of the slot's thread); for a raw text its DENSE_USER code, or the raw word if the
// text is not a dense user's - unless this thread has already asked the ID table for it (set_table),
// then that answer (a table number, or a LOCAL number of THIS thread). A user's rows are neighbours but
// change between the user's aliases (texts) from row to row, so the answers are kept in a small table
// indexed by a hash of the text; a row costs one probe of it. (Slots resolved on their line have no
// text and share entry 0; they are rare where it matters.)
struct RawCtx{
 static constexpr unsigned BITS=8;
 struct Entry{uint64_t raw;uint32_t user,code;};
 uint32_t dmax=0; // largest DENSE_USER code decoded here
 bool v3=false;
 Entry memo[1u<<BITS];
 RawCtx(){for(Entry& m:memo){m.raw=0;m.user=0xFFFFFFFFu;m.code=0;}} // no slot has that user word
 static inline __attribute__((always_inline)) size_t at(uint64_t raw){return (size_t)((raw*0x9E3779B97F4A7C15ULL)>>(64-BITS));}
 [[gnu::noinline]] uint32_t slow(Entry& m,uint32_t u,uint64_t sp){
  uint32_t code=u;
  if((u&DENSE_USER_RAW)==DENSE_USER_RAW){
   const uint32_t len=u&15u;
   code=v3?raw_dense_code_v3(sp,len):raw_dense_code_plain(sp,len);
   if(is_dense_code(code))dmax=max(dmax,code);
  }
  m.raw=sp;m.user=u;m.code=code;return code;
 }
 // sl must be the head of a 32-byte ESlot (the text is in its spare word).
 inline __attribute__((always_inline)) uint32_t code(const ESmall& sl){
  const uint32_t u=sl.user;const uint64_t sp=((const ESlot*)(const void*)&sl)->spare;
  Entry& m=memo[at(sp)];
  if(__builtin_expect((m.raw!=sp)|(m.user!=u),0))return slow(m,u,sp);
  return m.code;
 }
 void set_table(uint64_t sp,uint32_t len,uint32_t code){Entry& m=memo[at(sp)];m.raw=sp;m.user=DENSE_USER_RAW|len;m.code=code;}
};
// Sorts m distinct keys "click time << 32 | position" (m > 1, times below 2^32). A person's clicks are
// spread over the days before and inside the window, so one counting pass on the leading bits of the
// time (two buckets per key or more) leaves most keys alone in their bucket and an insertion pass
// finishes with few moves. If the times are bunched after all (a bucket with more than clickSortMax
// keys), std::sort does it. tmp: room for m keys; cnt: room for CLICK_SORT_BUCKETS + 1 counters.
static constexpr unsigned CLICK_SORT_BITS=13;static constexpr size_t CLICK_SORT_BUCKETS=size_t(1)<<CLICK_SORT_BITS;
static const uint32_t clickSortMax=[]{const char* v=getenv("SS_TEST_SORTMAX");return v?(uint32_t)strtoul(v,nullptr,10):16u;}(); // tests: 0 = always std::sort, 1 = on any shared bucket
// (TS: where the time starts in a key: 32 for "time << 32 | position", 28 for "time << 28 | position << 16 | campaign".)
template<unsigned TS> static inline __attribute__((always_inline)) void click_key_sort_t(uint64_t* const a,const size_t m,uint64_t* const tmp,uint32_t* const cnt){
 uint64_t mn=a[0],mx=a[0];
 for(size_t i=1;i<m;++i){mn=min(mn,a[i]);mx=max(mx,a[i]);}
 const uint64_t t0=mn>>TS,range=(mx>>TS)-t0;
 const unsigned lb=min<unsigned>((unsigned)bit_width(m-1)+1,CLICK_SORT_BITS),rb=(unsigned)bit_width(range),sh=rb>lb?rb-lb:0;
 const size_t B=(size_t)(range>>sh)+1; // buckets in use, at most 1 << lb
 memset(cnt,0,(B+1)*sizeof(uint32_t));
 uint32_t most=0;
 for(size_t i=0;i<m;++i){const uint32_t c=++cnt[(((a[i]>>TS)-t0)>>sh)+1];most=max(most,c);}
 if(__builtin_expect(most>clickSortMax,0)){sort(a,a+m);return;}
 for(size_t b=1;b<B;++b)cnt[b]+=cnt[b-1]; // cnt[b]: where bucket b starts
 for(size_t i=0;i<m;++i){const uint64_t k=a[i];tmp[cnt[((k>>TS)-t0)>>sh]++]=k;}
 a[0]=tmp[0];
 for(size_t i=1;i<m;++i){ // keys only move inside their bucket
  const uint64_t k=tmp[i];size_t j=i;
  while(j>0&&a[j-1]>k){a[j]=a[j-1];--j;}
  a[j]=k;
 }
}
[[gnu::noinline]] static void click_key_sort(uint64_t* const a,const size_t m,uint64_t* const tmp,uint32_t* const cnt){click_key_sort_t<32>(a,m,tmp,cnt);}
[[gnu::noinline]] static void click_key_sort28(uint64_t* const a,const size_t m,uint64_t* const tmp,uint32_t* const cnt){click_key_sort_t<28>(a,m,tmp,cnt);}
// Where one thread puts the session lists of the runs it reads (blocks of its pool) and a run's purchases.
struct SessArena{uint64_t* cur=nullptr;uint64_t* end=nullptr;vector<uint64_t> pur;};
// The event sweep of the counters [lo,hi) of an event slot array (stride 1 << evShift bytes, layout ESmall
// at the head of each slot). A run is a stretch of valid slots of one user index, not broken by a valid
// slot of anybody else; it belongs to the piece it starts in and may run past `hi` (cross), up to evEnd,
// so a piece first steps over the run that reaches into it. While a run is read its sessions are made as
// well: every row either continues the current session or starts one (more than GAP after the row
// before), written without a branch on that - each row writes "a session starts here" into the next free
// entry, and the entry only stays if the row really starts one. The run's counts, alias mask and session
// list go to the index's DInfo.
//   code_of(slot)     the user word of a valid slot (see RawCtx::code; plain `user` when slots are resolved)
//   run_start(code)   called with the first row's DENSE_USER code of every run
//   other(slot,code)  a valid slot of this piece whose user is not a dense user
// Returns the number of rows in the runs of this piece.
template<class CODE,class START,class OTHER>
static inline __attribute__((always_inline)) uint64_t sweep_event_runs(const char* const evBase,const unsigned evShift,const size_t lo,const size_t hi,const size_t evEnd,const bool cross,
  DInfo* const info,HugePool& pool,SessArena& ar,CODE&& code_of,START&& run_start,OTHER&& other){
 const auto ev_at=[evBase,evShift](size_t c)->const ESmall&{return *(const ESmall*)(const void*)(evBase+(c<<evShift));};
 uint64_t rows=0;
 size_t c=lo;
 if(cross&&lo>0){
  size_t p=lo;uint32_t code0=0;bool reaches=false;
  while(p>0){--p;const ESmall& sl=ev_at(p);if(sl.k>=PR_KBIAS){const uint32_t u=code_of(sl);if(is_dense_code(u)){code0=u&~7u;reaches=true;}break;}}
  if(reaches)for(;c<hi;++c){const ESmall& sl=ev_at(c);if(sl.k>=PR_KBIAS&&(code_of(sl)&~7u)!=code0)break;}
 }
 const size_t stop=cross?evEnd:hi; // how far a run may go
 while(c<hi){
  const ESmall& sl=ev_at(c);const uint64_t k0=sl.k;
  if(k0<PR_KBIAS){++c;continue;} // empty, or present but not valid
  const uint32_t u=code_of(sl);
  if(!is_dense_code(u)){other(sl,u);++c;continue;}
  run_start(u);
  const uint32_t code=u&~7u;const size_t first=c;size_t lastSlot=c;
  // Room: one entry per slot looked at, at most, plus the end entry.
  if((size_t)(ar.end-ar.cur)<hi-c+2){const size_t want=max<size_t>(hi-c+2,size_t(1)<<19);ar.cur=(uint64_t*)pool.take(want*8);ar.end=ar.cur+want;}
  uint64_t* blk=ar.cur;size_t cap=(size_t)(ar.end-blk);
  size_t n=0,ns=0;uint64_t last=0-(uint64_t)(GAP+1),disorder=0;uint32_t mask=0;ar.pur.clear();
  size_t j=c;
  const size_t stride=size_t(1)<<evShift;
  for(;;){
   const size_t lim=min(stop,c+cap-2);
   const char* p=(const char*)&ev_at(j);const char* const pe=(const char*)&ev_at(lim);const char* lastp=p;
   static constexpr uint32_t ALIAS_BIT[8]={1,2,4,8,16,32,64,128}; // (a table: one instruction instead of three for 1 << al)
   uint64_t* out=blk+ns; // the next free entry (a moving pointer: blk and ns need no register in the loop)
   for(;p<pe;p+=stride){
    const ESmall& r=*(const ESmall*)(const void*)p;const uint64_t k=r.k;
    if(k<PR_KBIAS)continue;
    const uint32_t al=code_of(r)^code; // the alias if the row is this index's
    if(al>7)break;
    mask|=ALIAS_BIT[al];
    const uint64_t ts=(k-PR_KBIAS)>>EV_TS,step=ts-last;
    disorder|=step; // its top bit: a row before its predecessor
    *out=(ts<<24)|n;out+=step>(uint64_t)GAP;last=ts;
    if(__builtin_expect((k&EV_PURCHASE)!=0,0))ar.pur.push_back(((uint64_t)(out-blk-1)<<40)|((uint64_t)r.cat<<24)|(k&EV_PRICE));
    ++n;lastp=p;
   }
   ns=(size_t)(out-blk);
   j=(size_t)(p-(const char*)&ev_at(0))>>evShift;
   if(n)lastSlot=(size_t)(lastp-(const char*)&ev_at(0))>>evShift;
   if(p<pe)goto run_done; // a valid slot of somebody else
   if(j>=stop)break;
   {const size_t cap2=cap*2;uint64_t* const nb=(uint64_t*)pool.take(cap2*8);memcpy(nb,blk,(ns+1)*8);blk=nb;cap=cap2;ar.end=nb+cap2;} // a long run past the piece: a bigger block
  }
  run_done:;
  {const size_t np=ar.pur.size(),need=ns+1+np+1;
   if(cap<need){uint64_t* const nb=(uint64_t*)pool.take(need*8);memcpy(nb,blk,(ns+1)*8);blk=nb;cap=need;ar.end=nb+need;}
   blk[ns]=n;if(np)memcpy(blk+ns+1,ar.pur.data(),np*8);blk[ns+1+np]=~0ull;
   ar.cur=blk+need;}
  DInfo& f=info[(code&~DENSE_USER)>>dn::ALIAS_BITS];
  dense_note_first(f.ef,(uint32_t)first+1);dense_note_end(f.el,(uint32_t)lastSlot+1);f.ec.fetch_add((uint32_t)n,memory_order_relaxed);
  uint64_t* none=nullptr;
  if(f.sess.compare_exchange_strong(none,blk,memory_order_relaxed)){
   f.ns.store((uint32_t)ns|(((disorder>>63)||n>=(size_t(1)<<24))?0x80000000u:0u),memory_order_relaxed);f.amask.store((uint8_t)mask,memory_order_relaxed);}
  else f.multi.store(1,memory_order_relaxed); // the index has another run
  rows+=n;
  c=j;
 }
 return rows;
}
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
 if(__builtin_expect(s0[0]=='e'&&parse36_v3(s0+1,l0-1,y)&&(ve=dn::index_of(y,lp.ke))<lp.limE,1)){if(!lp.strm)__builtin_prefetch(&lp.ev[ve],1,3);return (uint32_t)ve;}
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
// ---- Batch event loop (SS_LINES=batch; layout and contract in /home/claude/vert/BATCH.md) ----
// Stage 1 cuts a line as event_line_v3 does and, if its shapes fit, only writes the operands of the row into
// a batch of eight; stage 2 works out counter, ingest_ts, validity and k for the whole batch (the place for
// the vertical kernels; ev_stage2_scalar is the reference, built from the functions of the line code);
// stage 3 asks for the slots of the batch just decoded and then finishes the batch before it. A line whose
// shapes do not fit, and a row the stage-2 code refuses, goes through event_line_v3 when its batch is finished.
struct alignas(64) EvBatch{uint64_t idw[8],idn[8],ig0[8],ig1[8],ig2[8],ts0[8],ts1[8],ts2[8],tsn[8],kw[8],kn[8],fl[8];};
struct alignas(64) EvBatchOut{uint64_t ve[8],ingest[8],k[8],kind[8];uint8_t ok,idok;}; // idok: ve is the counter of a dense ID (also when ok is clear)
// What the loop keeps per row besides the operands.
struct EvRows{const char* line[8];const char* nl[8];const char* s5[8];const char* s6[8];uint64_t uw[8];uint32_t l5[8],l6[8],un[8],pre[8];unsigned n;uint8_t slow;};
SS_V3 static void ev_stage2_scalar(const EvBatch& in,const unsigned n,EvBatchOut& out,const EvLoop& lp){
 unsigned ok=0,idok=0;
 for(unsigned i=0;i<n;++i){
  out.ve[i]=0;out.ingest[i]=0;out.k[i]=0;out.kind[i]=0;
  uint64_t y,ve=0;bool good=true;
  if(parse36_v3((const char*)&in.idw[i],(unsigned)in.idn[i],y)&&(ve=dn::index_of(y,lp.ke))<lp.limE){idok|=1u<<i;out.ve[i]=ve;}else good=false;
  char ig[32]={0},ts[32]={0};
  memcpy(ig,&in.ig0[i],8);memcpy(ig+8,&in.ig1[i],8);memcpy(ig+12,&in.ig2[i],8);
  int64_t tod,day=0,ingest=0,tsv=0;
  if(ig[19]=='Z'&&(tod=tod_v3(ig))>=0&&date3(ig,lp.memo,day))ingest=day+tod;else good=false;
  if(in.tsn[i]==20){
   memcpy(ts,&in.ts0[i],8);memcpy(ts+8,&in.ts1[i],8);memcpy(ts+12,&in.ts2[i],8);
   int64_t tod2;
   if(ts[19]=='Z'&&memcmp(ts,ig,10)==0&&(tod2=tod_v3(ts))>=0)tsv=day+tod2;else good=false;
  }else{ // 13 digits
   memcpy(ts,&in.ts0[i],8);memcpy(ts+5,&in.ts1[i],8);
   if(!epoch13_v3(ts,tsv))good=false;
  }
  if(!good)continue;
  char kb[16]={0};memcpy(kb,&in.kw[i],8);
  const unsigned kind=in.kn[i]<=8?event_kind(string_view(kb,(size_t)in.kn[i])):0;
  const uint64_t tsrel=(uint64_t)tsv-(uint64_t)lp.wstart;
  const bool valid=kind&&(in.fl[i]&1)&&(in.fl[i]&2)&&tsrel<lp.wlen&&ingest-tsv<=DAY;
  out.ingest[i]=(uint64_t)ingest;out.kind[i]=kind;out.k[i]=valid?(tsrel+1)<<EV_TS:0;ok|=1u<<i;
 }
 out.ok=(uint8_t)ok;out.idok=(uint8_t)idok;
}
// Stage 1 for the line at p (b, w1, w2, nl as for event_line_v3<true,...>): row r of the batch, or a slow row.
// Returns false if the line is dropped outright (no event_id).
// Eight bytes as they stand (a plain load; memcpy into an indexed array would become a checked library call).
static inline __attribute__((always_inline)) uint64_t ld8(const char* p){uint64_t w;__builtin_memcpy(&w,p,8);return w;}
SS_V3I bool ev_stage1_v3(const char* const p,const char* const b,const WinMasks w1,const WinMasks w2,const char* const nl,EvBatch& bt,EvRows& rw,const unsigned r,const EvLoop& lp){
 rw.line[r]=p;rw.nl[r]=nl;
 const uint64_t c=w1.c,qa=w1.q;
 const uint64_t cA=_blsr_u64(c),cB=_blsr_u64(cA),cR=_blsr_u64(cB);
 const uint64_t m1=_blsmsk_u64(cB);
 const uint64_t q1=qa&m1,st1=((c^cB)<<1)|1,en1=c^cR;
 uint64_t sq1,op1;const uint64_t err1=quote_errors_v3(q1,st1,en1,sq1,op1);
 const uint64_t e2=_tzcnt_u64(cB);
 const uint64_t L=(uint64_t)(nl-b),nbit=1ull<<(L&63),keep=nbit-1;
 const uint64_t d=w2.c&keep,qb=w2.q&keep;
 const uint64_t st2=(d<<1)|1,en2=d|nbit;
 uint64_t sq2,op2;const uint64_t err2=quote_errors_v3(qb,st2,en2,sq2,op2);
 bool fast=(err1|err2|(uint64_t)(_mm_popcnt_u64(d)^3)|((e2|L)>>6))==0;
 if(__builtin_expect(fast,1)){
  uint64_t B1=st1+sq1,E1=en1-(op1>>1),B2=st2+sq2,E2=en2-(op2>>1);
  const unsigned z0=(unsigned)(q1&1);
  const char* const s0=p+z0;const unsigned l0=(unsigned)_tzcnt_u64(E1)-z0;
  if(l0==0)return false; // no event_id: dropped
  const unsigned z3=(unsigned)(qb&1);
  const char* const s3=b+z3;const unsigned l3=(unsigned)_tzcnt_u64(E2)-z3;
  B1=_blsr_u64(B1);E1=_blsr_u64(E1);
  const unsigned b1=(unsigned)_tzcnt_u64(B1),l1=(unsigned)_tzcnt_u64(E1)-b1;
  B1=_blsr_u64(B1);E1=_blsr_u64(E1);
  const unsigned b2=(unsigned)_tzcnt_u64(B1),l2=(unsigned)_tzcnt_u64(E1)-b2;
  B2=_blsr_u64(B2);E2=_blsr_u64(E2);
  const unsigned b4=(unsigned)_tzcnt_u64(B2),l4=(unsigned)_tzcnt_u64(E2)-b4;
  B2=_blsr_u64(B2);E2=_blsr_u64(E2);
  const unsigned b5=(unsigned)_tzcnt_u64(B2),l5=(unsigned)_tzcnt_u64(E2)-b5;
  B2=_blsr_u64(B2);E2=_blsr_u64(E2);
  const unsigned b6=(unsigned)_tzcnt_u64(B2),l6=(unsigned)_tzcnt_u64(E2)-b6;
  const char* const s1=p+b1;const char* const s2=p+b2;
  // the shapes of the fast path: 'e' + 1-8 bytes, ingest_ts 20 long, ts 20 or 13 long, user empty or 'u' + 0-8 bytes
  fast=(s0[0]=='e')&(l0-2u<8u)&(l3==20)&((l2==20)|(l2==13))&((l1==0)|((s1[0]=='u')&(l1<=9u)));
  if(__builtin_expect(fast,1)){
      bt.idw[r]=ld8(s0+1);bt.idn[r]=l0-1;
   bt.ig0[r]=ld8(s3);bt.ig1[r]=ld8(s3+8);bt.ig2[r]=ld8(s3+12);
   bt.ts0[r]=ld8(s2);bt.ts1[r]=ld8(s2+(l2==20?8:5));bt.ts2[r]=ld8(s2+12);bt.tsn[r]=l2;
   bt.kw[r]=ld8(b+b4);bt.kn[r]=l4;
   bt.fl[r]=(uint64_t)(l1!=0)|((uint64_t)(l5!=0)<<1);
   rw.un[r]=l1?l1-1:0;rw.uw[r]=_bzhi_u64(ld8(s1+1),8*rw.un[r]);
   rw.s5[r]=b+b5;rw.l5[r]=l5;rw.s6[r]=b+b6;rw.l6[r]=l6;
   return true;
  }
 }
 // not for the batch: the line code takes it when the batch is finished; its slot is asked for now
 rw.slow|=(uint8_t)(1u<<r);rw.pre[r]=event_pre_v3(p,lp);
 bt.idw[r]=0;bt.idn[r]=0;bt.ig0[r]=0;bt.ig1[r]=0;bt.ig2[r]=0;bt.ts0[r]=0;bt.ts1[r]=0;bt.ts2[r]=0;bt.tsn[r]=20;bt.kw[r]=0;bt.kn[r]=0;bt.fl[r]=0;
 return true;
}
// Stage 3 for one batch: every row in line order.
#define SS_BATCH_FINISH(X,MASKS) {const EvRows& R_=rw[X];const EvBatchOut& O_=bo[X]; \
  for(unsigned i_=0;i_<R_.n;++i_){ \
   if(__builtin_expect(((R_.slow>>i_)&1)|(~(O_.ok>>i_)&1),0)){ \
    const char* const q_=R_.line[i_];const WinMasks w1_=MASKS(q_);const char* const bb_=event_window2(q_,w1_.c); \
    const uint32_t pre_=((R_.slow>>i_)&1)?R_.pre[i_]:(((O_.idok>>i_)&1)?(uint32_t)O_.ve[i_]:NOPRE); \
    event_line_v3<true,true>(q_,bb_,w1_,MASKS(bb_),R_.nl[i_],pre_,lp,pend,pp,dmax,send,ring,rpos,general);continue;} \
   uint64_t k_=O_.k[i_];unsigned cat_=0; \
   if(k_&&O_.kind[i_]==3){int32_t price_=0; \
    if(price_value(string_view(R_.s6[i_],R_.l6[i_]),price_)){const uint32_t ci_=products.find(string_view(R_.s5[i_],R_.l5[i_]));cat_=ci_==KeyTable::NONE?0:(unsigned)(uint16_t)ci_;k_|=EV_PURCHASE|(uint64_t)(uint32_t)price_;} \
    else k_=0;} \
   const uint64_t ve_=O_.ve[i_];send=max(send,(uint32_t)ve_+1); \
   eslot_put(lp,ve_,k_?k_:ESLOT_INVALID,(k_?(uint64_t)(DENSE_USER_RAW|R_.un[i_]):0)|((uint64_t)cat_<<32)|lp.tid48,(int64_t)O_.ingest[i_],k_?R_.uw[i_]:0);} }
// Stage 2 and the slot requests for the batch just filled, then stage 3 for the one before.
#define SS_BATCH_TURN(MASKS) {EvRows& R_=rw[cur];for(unsigned i_=R_.n;i_<8;++i_){R_.slow|=(uint8_t)(1u<<i_);bt[cur].idn[i_]=0;bt[cur].tsn[i_]=20;} \
  ev_stage2_scalar(bt[cur],R_.n,bo[cur],lp); \
  if(!lp.strm)for(unsigned i_=0;i_<R_.n;++i_)if((bo[cur].idok>>i_)&~(R_.slow>>i_)&1)__builtin_prefetch(&lp.ev[bo[cur].ve[i_]],1,3); \
  SS_BATCH_FINISH(cur^1,MASKS) cur^=1;rw[cur].n=0;rw[cur].slow=0;}
#define SS_EVENT_LOOP_BATCH(NAME,TARGET,MASKS,NLMASK) \
template<class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const char* const end=base+len; \
 const EvLoop lp(tl,tab,products,PE,end);PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;ERing* const ring=nullptr;unsigned rpos=0; \
 EvBatch bt[2];EvBatchOut bo[2];EvRows rw[2];int cur=0;rw[0].n=0;rw[0].slow=0;rw[1].n=0;rw[1].slow=0; \
 const char* starts[IX_BLOCK+64];const char* scan=p; \
 while(p<lim){ \
  size_t n=0; \
  for(const char* const stop=scan+IX_BLOCK;scan<stop&&scan<end;scan+=64){uint64_t m=NLMASK(scan);while(m){starts[n++]=scan+_tzcnt_u64(m)+1;m=_blsr_u64(m);}} \
  for(size_t i=0;i<n&&p<lim;++i){ \
   const WinMasks w1=MASKS(p);const char* const b=event_window2(p,w1.c); \
   if(ev_stage1_v3(p,b,w1,MASKS(b),starts[i]-1,bt[cur],rw[cur],rw[cur].n,lp)&&++rw[cur].n==8)SS_BATCH_TURN(MASKS) \
   p=starts[i];} \
  if(scan>=end&&p<lim){p=line_general<7>(p,end,general);break;} /* the last line has no newline */ \
 } \
 SS_BATCH_TURN(MASKS) SS_BATCH_FINISH(cur^1,MASKS) \
 tl.denseMax=max(tl.denseMax,dmax);tl.evEnd=max(tl.evEnd,send); \
}
SS_EVENT_LOOP_BATCH(events_avx2_bt,SS_V3,win_masks_avx2,nl_mask_avx2)
SS_EVENT_LOOP_BATCH(events_avx512z_bt,SS_V5,win_masks_avx512z,nl_mask_avx512z)
SS_EVENT_LOOP_BATCH(events_avx512y_bt,SS_V5,win_masks_avx512y,nl_mask_avx512y)
// ---- ev8 event loop (SS_LINES=ev8) ----
// The lines are taken eight at a time straight from the line index: a kernel works out everything about the
// eight rows from the line text (vert::scan8_v5 + dec8_v5, AVX-512, eight 64-bit lanes; vert::ev8_ref is the scalar twin
// with the same decisions, used by the avx2 loop), the slots of the group are asked for, and then the group
// before it is finished in line order, so every slot has had at least eight lines of time. A row the kernel
// does not decide (ok bit clear) goes through event_line_v3 unchanged when its group is finished. The
// kernel's answers for an ok row are exactly what event_line_v3<true,true> would hand to eslot_commit for
// that line, a purchase's category excepted (see pur); the contract is the comment of Ev8Out below and the
// head of /home/claude/vert/vert.h.
// Option set of the pasted kernel. Bit 0: the event_id and the user's letter come from the first 16 bytes of
// each line, which the scan keeps, instead of three gathers (a user letter beyond them leaves the row to the
// line code). Bit 1: when every Z/offset-form ts of a group has its ingest_ts date text, the date lookup of
// ingest_ts is reused instead of three more gathers. Same results for every set (the kernel's own tests ran
// all four). Set 3 measured fastest on the scoring machine, where gathers are slow: 37.9 ns per row against
// 43.8 for set 0. -DVERT_EV8_OPT=n overrides.
#ifndef VERT_EV8_OPT
#define VERT_EV8_OPT 3
#endif
// ---- ev8: eight event lines per call (vertical AVX-512). Generated from /home/claude/vert/vert.h by mkpaste.py; contract in
// ---- vert.h / BATCH.md. Tested as this exact text by vert_line_test -DVERT_PASTE. Do not edit here: change vert.h and regenerate.
namespace vert{
#define VERT_T5 __attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512cd,avx2,bmi,bmi2,lzcnt,popcnt")))
static constexpr uint64_t MASK40=(1ull<<40)-1;
static constexpr uint64_t M1=0x9E3779B97F4Bull&MASK40,M2=0xD6E8FEB86659ull&MASK40;
static constexpr uint64_t inv40(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&MASK40;}
static constexpr uint64_t I1=inv40(M1),I2=inv40(M2);
static_assert(((M1*I1)&MASK40)==1&&((M2*I2)&MASK40)==1);
static constexpr int64_t DAYMS=86400000LL;
static constexpr unsigned EV_TS_SHIFT=25; // main.cpp EV_TS
static constexpr uint64_t ESLOT_INVALID_K=2,EV_PURCHASE_BIT=1ull<<24,RAW_USER=0xC0000000u,PRICE_MAX=10000000; // main.cpp: ESLOT_INVALID, EV_PURCHASE, DENSE_USER_RAW, price_value's limit
static inline uint64_t index_of_ref(uint64_t y,uint64_t key){uint64_t m=(y*I2)&MASK40;m=m^(m>>17)^(m>>34);return ((m^key)*I1)&MASK40;}
static inline unsigned id8_ref(const uint64_t* w,const uint64_t* n,uint64_t key,uint64_t limit,uint64_t* ctr){
 unsigned acc=0;
 for(int i=0;i<8;++i){
  ctr[i]=0;
  const uint64_t len=n[i];
  if(len-1u>7u)continue;
  unsigned char s[8];memcpy(s,&w[i],8);
  if(s[0]=='0'&&len>1)continue;
  uint64_t v=0;bool ok=true;
  for(uint64_t j=0;j<len;++j){unsigned d=s[j]-(unsigned)'0';if(d>9){d=s[j]-(unsigned)'a';if(d>25){ok=false;break;}d+=10;}v=v*36+d;}
  if(!ok||v>MASK40)continue;
  const uint64_t c=index_of_ref(v,key);
  if(c<limit){ctr[i]=c;acc|=1u<<i;}
 }
 return acc;
}
static const uint64_t VKT[]={
 /* 0*/ (uint64_t)(64),
 /* 1*/ (uint64_t)(uint8_t)(0x30)*0x0101010101010101ULL,
 /* 2*/ (uint64_t)(uint8_t)('0')*0x0101010101010101ULL,
 /* 3*/ (uint64_t)(uint8_t)('a')*0x0101010101010101ULL,
 /* 4*/ (uint64_t)(uint8_t)(9)*0x0101010101010101ULL,
 /* 5*/ (uint64_t)(uint8_t)(25)*0x0101010101010101ULL,
 /* 6*/ (uint64_t)(uint8_t)((char)(10-'a'))*0x0101010101010101ULL,
 /* 7*/ (uint64_t)(uint16_t)(0x0124)*0x0001000100010001ULL,
 /* 8*/ (uint64_t)(uint32_t)(0x00010510)*0x0000000100000001ULL,
 /* 9*/ (uint64_t)(1679616),
 /*10*/ (uint64_t)(1),
 /*11*/ (uint64_t)(7),
 /*12*/ (uint64_t)(MASK40),
 /*13*/ (uint64_t)(I2),
 /*14*/ (uint64_t)(I1),
 /*15*/ (uint64_t)(0x0000000030540000ULL),
 /*16*/ (uint64_t)(0xFFFFFFFF0200FFFFULL),
 /*17*/ (uint64_t)(0x0030303A30303A30ULL),
 /*18*/ (uint64_t)(0xFF09050009050009ULL),
 /*19*/ (uint64_t)(0x0000000064000000ULL),
 /*20*/ (uint64_t)(0x0000000001680000ULL),
 /*21*/ (uint64_t)(0x00010A00010A0001ULL),
 /*22*/ (uint64_t)(0x00010001003C0E10ULL),
 /*23*/ (uint64_t)(1000),
 /*24*/ (uint64_t)(86400000),
 /*25*/ (uint64_t)(0x010A010A010A010AULL),
 /*26*/ (uint64_t)(0x0001006400010064ULL),
 /*27*/ (uint64_t)(0x010A010A01000000ULL),
 /*28*/ (uint64_t)(0x0001006400010000ULL),
 /*29*/ (uint64_t)(10000),
 /*30*/ (uint64_t)(100000),
 /*31*/ (uint64_t)(0x30303A3030000000ULL),
 /*32*/ (uint64_t)(0x0905000902FFFFFFULL),
 /*33*/ (uint64_t)(0x010A00010A000000ULL),
 /*34*/ (uint64_t)(0x0001003C003C0000ULL),
 /*35*/ (uint64_t)(60000),
 /*36*/ (uint64_t)(0x40),
 /*37*/ (uint64_t)(0x3C),
 /*38*/ (uint64_t)(0x0F03),
 /*39*/ (uint64_t)(0xFFFFFFFFULL),
 /*40*/ (uint64_t)(0xFFFF),
 /*41*/ (uint64_t)(4),
 /*42*/ (uint64_t)(0x77656976ULL),
 /*43*/ (uint64_t)(0x74726163ULL),
 /*44*/ (uint64_t)(8),
 /*45*/ (uint64_t)(0x6573616863727570ULL),
 /*46*/ (uint64_t)(2),
 /*47*/ (uint64_t)(3),
 /*48*/ (uint64_t)('Z'),
 /*49*/ (uint64_t)('+'),
 /*50*/ (uint64_t)('-'),
 /*51*/ (uint64_t)(20),
 /*52*/ (uint64_t)(25),
 /*53*/ (uint64_t)(13),
 /*54*/ (uint64_t)(DAYMS),
 /*55*/ (uint64_t)(uint8_t)(',')*0x0101010101010101ULL,
 /*56*/ (uint64_t)(uint8_t)('"')*0x0101010101010101ULL,
 /*57*/ (uint64_t)(-1),
 /*58*/ (uint64_t)(63),
 /*59*/ (uint64_t)(16),
 /*60*/ (uint64_t)(0xFF),
 /*61*/ (uint64_t)('e'),
 /*62*/ (uint64_t)('u'),
 /*63*/ (uint64_t)(9),
 /*64*/ (uint64_t)(12),
 /*65*/ (uint64_t)(5),
 /*66*/ (uint64_t)(17),
 /*67*/ (uint64_t)(PRICE_MAX-1),
 /*68*/ (uint64_t)(EV_PURCHASE_BIT),
 /*69*/ (uint64_t)(ESLOT_INVALID_K),
 /*70*/ (uint64_t)(RAW_USER),
};
static inline __attribute__((always_inline)) const uint64_t* vk_ptr(){const uint64_t* p=VKT;__asm__("":"+r"(p));return p;}
#define VERT_K5(i) _mm512_set1_epi64((long long)VK[i])
VERT_T5 static inline __m512i id8k_v5(__m512i W,__m512i N,uint64_t key,uint64_t limit,__mmask8& okOut){
 const uint64_t* const VK=vk_ptr();
 const __m512i n8=_mm512_slli_epi64(N,3);
 const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(W,_mm512_sub_epi64(VERT_K5(0),n8)),_mm512_srlv_epi64(VERT_K5(1),n8));
 const __m512i t=_mm512_sub_epi8(x,VERT_K5(2)),u=_mm512_sub_epi8(x,VERT_K5(3));
 const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(t,VERT_K5(4)),_mm512_subs_epu8(u,VERT_K5(5)));
 const __m512i d=_mm512_min_epu8(t,_mm512_add_epi8(x,VERT_K5(6)));
 const __m512i p2=_mm512_maddubs_epi16(d,VERT_K5(7));
 const __m512i p4=_mm512_madd_epi16(p2,VERT_K5(8));
 const __m512i val=_mm512_add_epi64(_mm512_mul_epu32(p4,VERT_K5(9)),_mm512_srli_epi64(p4,32));
 // MIN36[n] picked by the low three bits of n (n == 8 -> entry 0); n == 0 and n > 8 are refused by the length test
 const __m512i min36=_mm512_permutexvar_epi64(N,_mm512_setr_epi64(78364164096LL,0,36,1296,46656,1679616,60466176,2176782336LL));
 __mmask8 ok=_mm512_testn_epi64_mask(bad,bad);
 // (each test is a compare under the mask of the ones before: the mask never leaves the mask registers)
 ok=_mm512_mask_cmple_epu64_mask(ok,_mm512_sub_epi64(N,VERT_K5(10)),VERT_K5(11));
 ok=_mm512_mask_cmpge_epu64_mask(ok,val,min36);
 ok=_mm512_mask_cmple_epu64_mask(ok,val,VERT_K5(12));
 const __m512i mask=VERT_K5(12);
 __m512i m=_mm512_and_si512(_mm512_mullo_epi64(val,VERT_K5(13)),mask);
 m=_mm512_ternarylogic_epi64(m,_mm512_srli_epi64(m,17),_mm512_srli_epi64(m,34),0x96);
 m=_mm512_xor_si512(m,_mm512_set1_epi64((long long)key));
 const __m512i c=_mm512_and_si512(_mm512_mullo_epi64(m,VERT_K5(14)),mask);
 okOut=_mm512_mask_cmplt_epu64_mask(ok,c,_mm512_set1_epi64((long long)limit));return c;
}
struct DateEnt{uint64_t k;uint32_t cc;uint32_t pad;int64_t ms;};
static_assert(sizeof(DateEnt)==24);
struct EvConst{uint64_t ke,limE;int64_t wstart;uint64_t wlen;const DateEnt* memo;uint64_t tid48=0;}; // tid48 (= lp.tid48) is used by ev8 only
static inline unsigned date_slot(uint64_t w0,uint64_t w1){ // w0 = date bytes 0..7, w1 = bytes 8..15
 return (unsigned)(((w0>>34)&0x40)|((w0>>46)&0x3C)|(w1&0x0F03));
}
static inline unsigned char byte_of(uint64_t w,unsigned i){return (unsigned char)(w>>(8*i));}
static inline bool tod_ref(uint64_t w1,uint64_t w2,uint64_t& ms){
 if(byte_of(w1,2)!='T'||byte_of(w2,1)!=':'||byte_of(w2,4)!=':')return false;
 const unsigned ht=byte_of(w1,3)-(unsigned)'0',hu=byte_of(w2,0)-(unsigned)'0',mt=byte_of(w2,2)-(unsigned)'0',mu=byte_of(w2,3)-(unsigned)'0',st=byte_of(w2,5)-(unsigned)'0',su=byte_of(w2,6)-(unsigned)'0';
 if(ht>2||hu>9||mt>5||mu>9||st>5||su>9)return false;
 const uint64_t v=(uint64_t)(((ht*10+hu)*60+mt*10+mu)*60+st*10+su)*1000;
 if(v>=86400000ULL)return false;
 ms=v;return true;
}
static inline bool day_ref(uint64_t w0,uint64_t w1,const DateEnt* memo,uint64_t& dayms){
 const DateEnt& x=memo[date_slot(w0,w1)];
 if(x.k==((w0>>16)|(w1<<48))&&x.cc==(uint32_t)(w0&0xFFFF)){dayms=(uint64_t)x.ms;return true;}
 return false;
}
static inline bool epoch_ref(uint64_t w0,uint64_t w1,uint64_t& ms){
 uint64_t hi=0,lo=0;
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w0,i)-(unsigned)'0';if(d>9)return false;hi=hi*10+d;}
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w1,i)-(unsigned)'0';if(d>9)return false;if(i>=3)lo=lo*10+d;}
 ms=hi*100000+lo;return true;
}
static inline bool off_ref(uint64_t w2,uint64_t w3,int64_t& delta){
 const unsigned char sg=byte_of(w2,7);
 if((sg!='+'&&sg!='-')||byte_of(w3,5)!=':')return false;
 const unsigned o1=byte_of(w3,3)-(unsigned)'0',o2=byte_of(w3,4)-(unsigned)'0',o3=byte_of(w3,6)-(unsigned)'0',o4=byte_of(w3,7)-(unsigned)'0';
 if(o1>9||o2>9||o3>9||o4>9)return false;
 const unsigned oh=o1*10+o2,om=o3*10+o4;
 if(oh>23||om>59)return false; // (ts_v3 says "not a timestamp" here; the kernels simply refuse the row)
 const int64_t off=(int64_t)(oh*60+om)*60000LL;
 delta=sg=='-'?off:-off;return true;
}
static inline unsigned kind_ref(uint64_t w,uint64_t n){
 if(n==4)return (uint32_t)w==0x77656976u?1u:((uint32_t)w==0x74726163u?2u:0u);
 if(n==8)return w==0x6573616863727570ULL?3u:0u;
 return 0;
}
#define VERT_C5(x) _mm512_set1_epi64((long long)(x))
#define VERT_LD5(p) _mm512_loadu_si512((const void*)(p))
VERT_T5 static inline __m512i tod8k_v5(__m512i a,__m512i b,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i va=_mm512_sub_epi8(a,VERT_K5(15)),la=VERT_K5(16);
 const __m512i vb=_mm512_sub_epi8(b,VERT_K5(17)),lb=VERT_K5(18);
 const __m512i pa=_mm512_madd_epi16(_mm512_maddubs_epi16(va,VERT_K5(19)),VERT_K5(20));
 const __m512i pb=_mm512_madd_epi16(_mm512_maddubs_epi16(vb,VERT_K5(21)),VERT_K5(22));
 const __m512i s=_mm512_add_epi32(pa,pb);
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(s,_mm512_srli_epi64(s,32)),VERT_K5(23));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(va,la),la),_mm512_max_epu8(vb,lb),lb),ms,VERT_K5(24));
 return ms;
}
VERT_T5 static inline __m512i epoch8k_v5(__m512i w0,__m512i w1,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i z=VERT_K5(2),nine=VERT_K5(4);
 const __m512i v0=_mm512_sub_epi8(w0,z),v1=_mm512_sub_epi8(w1,z);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v0,nine),nine),_mm512_max_epu8(v1,nine),nine);
 const __m512i p0=_mm512_madd_epi16(_mm512_maddubs_epi16(v0,VERT_K5(25)),VERT_K5(26));
 const __m512i p1=_mm512_madd_epi16(_mm512_maddubs_epi16(v1,VERT_K5(27)),VERT_K5(28));
 const __m512i tenk=VERT_K5(29);
 const __m512i hi=_mm512_add_epi64(_mm512_mul_epu32(p0,tenk),_mm512_srli_epi64(p0,32));
 const __m512i lo=_mm512_add_epi64(_mm512_mul_epu32(p1,tenk),_mm512_srli_epi64(p1,32));
 return _mm512_add_epi64(_mm512_mul_epu32(hi,VERT_K5(30)),lo);
}
VERT_T5 static inline __m512i off8k_v5(__m512i w3,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i v=_mm512_sub_epi8(w3,VERT_K5(31)),l=VERT_K5(32);
 const __m512i p=_mm512_madd_epi16(_mm512_maddubs_epi16(v,VERT_K5(33)),VERT_K5(34));
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(p,_mm512_srli_epi64(p,32)),VERT_K5(35));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v,l),l),ms,VERT_K5(24));
 return ms;
}
VERT_T5 static inline __m512i day8k_v5(__m512i w0,__m512i w1,const DateEnt* memo,__mmask8& hit){
 const uint64_t* const VK=vk_ptr();
 const __m512i slot=_mm512_ternarylogic_epi64(_mm512_and_si512(_mm512_srli_epi64(w0,34),VERT_K5(36)),_mm512_and_si512(_mm512_srli_epi64(w0,46),VERT_K5(37)),_mm512_and_si512(w1,VERT_K5(38)),0xFE);
 const __m512i i3=_mm512_add_epi64(slot,_mm512_slli_epi64(slot,1));
 const long long* base=(const long long*)memo;
 const __m512i gk=_mm512_i64gather_epi64(i3,base,8),gc=_mm512_i64gather_epi64(i3,base+1,8),gm=_mm512_i64gather_epi64(i3,base+2,8);
 const __m512i k=_mm512_or_si512(_mm512_srli_epi64(w0,16),_mm512_slli_epi64(w1,48));
 hit=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(gk,k),_mm512_and_si512(gc,VERT_K5(39)),_mm512_and_si512(w0,VERT_K5(40)));
 return gm;
}
VERT_T5 static inline __m512i kind8k_v5(__m512i w,__m512i n,__mmask8& nz){
 const uint64_t* const VK=vk_ptr();
 const __m512i lo=_mm512_and_si512(w,VERT_K5(39));
 const __mmask8 is4=_mm512_cmpeq_epi64_mask(n,VERT_K5(41));
 const __mmask8 view=_mm512_mask_cmpeq_epi64_mask(is4,lo,VERT_K5(42)),cart=_mm512_mask_cmpeq_epi64_mask(is4,lo,VERT_K5(43));
 const __mmask8 pur=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(n,VERT_K5(44)),w,VERT_K5(45));
 nz=view|cart|pur;
 __m512i kind=_mm512_maskz_mov_epi64(view,VERT_K5(10));
 kind=_mm512_mask_mov_epi64(kind,cart,VERT_K5(46));
 return _mm512_mask_mov_epi64(kind,pur,VERT_K5(47));
}
struct alignas(64) Ev8Out{
 uint64_t ve[8];      // idok rows: event counter (== event_pre_v3)
 uint64_t so[8];      // idok rows: ve << 5 (byte offset of the 32-byte ESlot)
 uint64_t kk[8];      // ok rows: the slot's k: ESLOT_INVALID (2) if not a valid event, else (ts - window_start + 1) << 25,
                      //          for a purchase with EV_PURCHASE | price already in it (final)
 uint64_t ingest[8];  // ok rows: ingest_ts, ms
 uint64_t uct[8];     // ok rows: valid ? (DENSE_USER_RAW | n) | tid48 : tid48   (n = bytes after 'u', 0..8); the caller ORs cat << 32 for pur rows
 uint64_t uw[8];      // ok rows: valid ? the n bytes after 'u', zero-padded : 0
 const char* s5[8];const char* s6[8];uint64_t l5[8],l6[8]; // ok rows: product_id / price_cents content (quotes stripped); needed for pur rows (s5,l5)
 uint8_t ok;          // bit i: row i is on event_line_v3's fast path and all of the above is exact
 uint8_t idok;        // bit i: ve[i] is exactly event_pre_v3's counter of the line (ok implies idok)
 uint8_t pur;         // bit i: ok row that is a valid purchase (price already checked and in kk): the caller looks the category up
};
static inline uint64_t ld8_ref(const char* p){uint64_t w;memcpy(&w,p,8);return w;}
static inline void masks_ref(const char* p,uint64_t& c,uint64_t& q){c=0;q=0;for(int i=0;i<64;++i){c|=(uint64_t)(p[i]==',')<<i;q|=(uint64_t)(p[i]=='"')<<i;}}
static inline unsigned tz_ref(uint64_t x){return x?(unsigned)__builtin_ctzll(x):64u;}
static inline uint64_t blsr_ref(uint64_t x){return x&(x-1);}
static inline void ev8_ref(const char* const* ls,const char* const* nx,const EvConst& c,Ev8Out& o){
 unsigned okm=0,idm=0,pum=0;
 for(int i=0;i<8;++i){
  o.ve[i]=o.so[i]=o.kk[i]=o.ingest[i]=o.uct[i]=o.uw[i]=o.l5[i]=o.l6[i]=0;o.s5[i]=o.s6[i]=ls[i];
  const char* const p=ls[i];const char* const nl=nx[i]-1;
  uint64_t cm,qa;masks_ref(p,cm,qa);
  // the counter as event_pre_v3 sees it: text before the first comma of the first 16 bytes, one quote pair stripped
  {const unsigned cp=tz_ref(cm)<16?tz_ref(cm):16;const unsigned z=(unsigned)(qa&1);
   const uint64_t n=(uint64_t)cp-2*z-1,w=ld8_ref(p+z+1);uint64_t ctr[8];const uint64_t ws[8]={w,w,w,w,w,w,w,w},ns[8]={n,n,n,n,n,n,n,n};
   if(p[z]=='e'&&(id8_ref(ws,ns,c.ke,c.limE,ctr)&1)){o.ve[i]=ctr[0];o.so[i]=ctr[0]<<5;idm|=1u<<i;}}
  // structure, as event_line_v3 (DREC form: the newline is known)
  const uint64_t cA=blsr_ref(cm),cB=blsr_ref(cA),cR=blsr_ref(cB);
  const uint64_t m1=cB^(cB-1);
  const uint64_t q1=qa&m1,st1=((cm^cB)<<1)|1,en1=cm^cR;
  const uint64_t sq1=q1&st1,op1=(sq1+~en1)&en1,err1=op1^((q1&~st1)<<1);
  const uint64_t e2=tz_ref(cB);
  const char* const b=p+e2+1;
  uint64_t c2,q2;masks_ref(b,c2,q2);
  const uint64_t L=(uint64_t)(nl-b),nbit=1ull<<(L&63),keep=nbit-1;
  const uint64_t d=c2&keep,qb=q2&keep;
  const uint64_t st2=(d<<1)|1,en2=d|nbit;
  const uint64_t sq2=qb&st2,op2=(sq2+~en2)&en2,err2=op2^((qb&~st2)<<1);
  if((err1|err2|(uint64_t)(__builtin_popcountll(d)^3)|((e2|L)>>6))!=0)continue;
  uint64_t B1=st1+sq1,E1=en1-(op1>>1),B2=st2+sq2,E2=en2-(op2>>1);
  const unsigned z0=(unsigned)(q1&1);
  const unsigned l0=tz_ref(E1)-z0;
  if(l0==0)continue;
  const unsigned z3=(unsigned)(qb&1);
  const char* const s3=b+z3;const unsigned l3=tz_ref(E2)-z3;
  B1=blsr_ref(B1);E1=blsr_ref(E1);
  const unsigned b1=tz_ref(B1),l1=tz_ref(E1)-b1;
  B1=blsr_ref(B1);E1=blsr_ref(E1);
  const unsigned b2=tz_ref(B1),l2=tz_ref(E1)-b2;
  B2=blsr_ref(B2);E2=blsr_ref(E2);
  const unsigned b4=tz_ref(B2),l4=tz_ref(E2)-b4;
  B2=blsr_ref(B2);E2=blsr_ref(E2);
  const unsigned b5=tz_ref(B2),l5=tz_ref(E2)-b5;
  B2=blsr_ref(B2);E2=blsr_ref(E2);
  const unsigned b6=tz_ref(B2),l6=tz_ref(E2)-b6;
  const char* const s1=p+b1;const char* const s2=p+b2;
  o.s5[i]=b+b5;o.l5[i]=l5;o.s6[i]=b+b6;o.l6[i]=l6;
  const unsigned kind=kind_ref(ld8_ref(b+b4),l4);
  if(!(idm>>i&1))continue;                                   // (then l0 is 2..9 and the ID is the line's first field)
  if(!(l1==0||(s1[0]=='u'&&l1<=9)))continue;                 // user: empty, or 'u' + 0..8 bytes
  const uint64_t un=l1?l1-1:0,uw=!l1?0:(un==8?ld8_ref(s1+1):(ld8_ref(s1+1)&((1ull<<(8*un))-1)));
  if(l3!=20)continue;
  const uint64_t ig0=ld8_ref(s3),ig1=ld8_ref(s3+8),ig2=ld8_ref(s3+12);
  uint64_t todi,dayi;
  if(!tod_ref(ig1,ig2,todi)||byte_of(ig2,7)!='Z'||!day_ref(ig0,ig1,c.memo,dayi))continue;
  const int64_t ingest=(int64_t)(dayi+todi);
  o.ingest[i]=(uint64_t)ingest;
  int64_t ts;
  if(l2==13){uint64_t v;if(!epoch_ref(ld8_ref(s2),ld8_ref(s2+5),v))continue;ts=(int64_t)v;}
  else if(l2==20||l2==25){
   const uint64_t t0=ld8_ref(s2),t1=ld8_ref(s2+8),t2=ld8_ref(s2+12);
   uint64_t tods,days;
   if(!tod_ref(t1,t2,tods)||!day_ref(t0,t1,c.memo,days))continue;
   ts=(int64_t)(days+tods);
   if(l2==20){if(byte_of(t2,7)!='Z')continue;}
   else{int64_t delta;if(!off_ref(t2,ld8_ref(s2+17),delta))continue;ts+=delta;}
  }else continue;
  const uint64_t tsrel=(uint64_t)ts-(uint64_t)c.wstart;
  uint64_t k=0;
  if(kind!=0&&l1!=0&&l5!=0&&tsrel<c.wlen&&ingest-ts<=DAYMS)k=(tsrel+1)<<EV_TS_SHIFT;
  if(k&&kind==3){ // price_value: digits only, 1..10000000 (longer than 8 bytes: left to the row code)
   if(l6>8)continue;
   uint64_t price=0;bool dig=l6!=0;
   for(unsigned j=0;j<l6;++j){const unsigned dd=(unsigned char)b[b6+j]-(unsigned)'0';if(dd>9)dig=false;price=price*10+dd;}
   if(dig&&price>=1&&price<=PRICE_MAX){k|=EV_PURCHASE_BIT|price;pum|=1u<<i;}else k=0;
  }
  okm|=1u<<i;
  o.kk[i]=k?k:ESLOT_INVALID_K;o.uct[i]=(k?(RAW_USER|un):0)|c.tid48;o.uw[i]=k?uw:0;
 }
 o.ok=(uint8_t)okm;o.idok=(uint8_t)idm;o.pur=(uint8_t)pum;
}
struct alignas(64) Ev8Scan{uint64_t c1[8],q1[8],c2[8],q2[8],b[8],h[16];}; // h: the first 16 bytes of each line (OPT bit 0 only)
#ifndef VERT_EV8_OPT
#define VERT_EV8_OPT 0
#endif
template<unsigned OPT> VERT_T5 static inline void scan8t_v5(const char* const* ls,Ev8Scan& s){
 const uint64_t* const VK=vk_ptr();
 const __m512i vc=VERT_K5(55),vq=VERT_K5(56);
 for(int i=0;i<8;++i){
  const char* const p=ls[i];
  const __m512i x=_mm512_loadu_si512((const void*)p);
  const uint64_t cm=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc));
  s.c1[i]=cm;s.q1[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq));
  if(OPT&1)_mm_storeu_si128((__m128i*)&s.h[2*i],_mm512_castsi512_si128(x));
  const char* const b=p+_tzcnt_u64(_blsr_u64(_blsr_u64(cm)))+1; // one past the third comma (p+65 if there is none)
  const __m512i y=_mm512_loadu_si512((const void*)b);
  s.c2[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vc));s.q2[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vq));s.b[i]=(uint64_t)b;
 }
}
template<unsigned OPT> VERT_T5 static inline void dec8t_v5(const char* const* ls,const char* const* nx,const Ev8Scan& s,const EvConst& c,Ev8Out& o){
 const uint64_t* const VK=vk_ptr();
 const uint64_t* const aC1=s.c1;const uint64_t* const aQ1=s.q1;const uint64_t* const aC2=s.c2;const uint64_t* const aQ2=s.q2;const uint64_t* const aB=s.b;
 // ---- vertical ----
 const __m512i one=VERT_K5(10),ones=VERT_K5(57),c63=VERT_K5(58),c64=VERT_K5(0);
 #define VERT_TZ5(x,xm) _mm512_sub_epi64(c63,_mm512_lzcnt_epi64(_mm512_xor_si512(x,xm))) /* tzcnt(x) given xm = x-1 (63 for x == 0) */
 const __m512i P=VERT_LD5(ls),NL=_mm512_add_epi64(VERT_LD5(nx),ones),Bv=_mm512_load_si512((const void*)aB);
 const __m512i C1=_mm512_load_si512((const void*)aC1),Q1=_mm512_load_si512((const void*)aQ1),C2=_mm512_load_si512((const void*)aC2),Q2=_mm512_load_si512((const void*)aQ2);
 // window 1: the first three commas, quote rules up to the third
 const __m512i t0=_mm512_add_epi64(C1,ones),c0=VERT_TZ5(C1,t0),cA=_mm512_and_si512(C1,t0);
 const __m512i t1=_mm512_add_epi64(cA,ones),c1=VERT_TZ5(cA,t1),cB=_mm512_and_si512(cA,t1);
 const __m512i t2=_mm512_add_epi64(cB,ones),m1=_mm512_xor_si512(cB,t2),cR=_mm512_and_si512(cB,t2);
 const __m512i c2=_mm512_add_epi64(_mm512_sub_epi64(Bv,P),ones); // tzcnt(cB), 64 if there is no third comma
 const __m512i q1=_mm512_and_si512(Q1,m1),st1=_mm512_or_si512(_mm512_slli_epi64(_mm512_xor_si512(C1,cB),1),one),en1=_mm512_xor_si512(C1,cR);
 const __m512i sq1=_mm512_and_si512(q1,st1),op1=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq1,en1),ones),en1);
 const __m512i err1=_mm512_xor_si512(op1,_mm512_slli_epi64(_mm512_andnot_si512(st1,q1),1));
 // window 2: the bytes before the newline, three commas, quote rules
 const __m512i L=_mm512_sub_epi64(NL,Bv),nbit=_mm512_sllv_epi64(one,_mm512_and_si512(L,c63)),keep=_mm512_add_epi64(nbit,ones);
 const __m512i d=_mm512_and_si512(C2,keep),qb=_mm512_and_si512(Q2,keep);
 const __m512i st2=_mm512_or_si512(_mm512_slli_epi64(d,1),one),en2=_mm512_or_si512(d,nbit);
 const __m512i sq2=_mm512_and_si512(qb,st2),op2=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq2,en2),ones),en2);
 const __m512i err2=_mm512_xor_si512(op2,_mm512_slli_epi64(_mm512_andnot_si512(st2,qb),1));
 const __m512i u0=_mm512_add_epi64(d,ones),d0=VERT_TZ5(d,u0),dA=_mm512_and_si512(d,u0);
 const __m512i u1=_mm512_add_epi64(dA,ones),d1=VERT_TZ5(dA,u1),dB=_mm512_and_si512(dA,u1);
 const __m512i u2=_mm512_add_epi64(dB,ones),d2=VERT_TZ5(dB,u2),dR=_mm512_and_si512(dB,u2);
 const __m512i err=_mm512_ternarylogic_epi64(err1,err2,dR,0xFE);            // a quote error, or a fourth comma before the newline
 __mmask8 ok=_mm512_testn_epi64_mask(err,err);
 ok=_mm512_mask_test_epi64_mask(ok,dB,dB);                                    // at least three commas
 ok=_mm512_mask_cmplt_epu64_mask(ok,_mm512_or_si512(c2,L),c64);               // third comma and newline inside their windows ((e2|L)>>6 == 0)
 // fields: a quoted field starts one later and is two shorter
 const __m512i z0=_mm512_and_si512(Q1,one);
 const __m512i a1=_mm512_add_epi64(c0,one),z1=_mm512_and_si512(_mm512_srlv_epi64(q1,a1),one),o1=_mm512_add_epi64(a1,z1),l1=_mm512_sub_epi64(_mm512_sub_epi64(c1,o1),z1);
 const __m512i a2=_mm512_add_epi64(c1,one),z2=_mm512_and_si512(_mm512_srlv_epi64(q1,a2),one),o2=_mm512_add_epi64(a2,z2),l2=_mm512_sub_epi64(_mm512_sub_epi64(c2,o2),z2);
 const __m512i z3=_mm512_and_si512(qb,one),l3=_mm512_sub_epi64(d0,_mm512_add_epi64(z3,z3));
 const __m512i a4=_mm512_add_epi64(d0,one),z4=_mm512_and_si512(_mm512_srlv_epi64(qb,a4),one),o4=_mm512_add_epi64(a4,z4),l4=_mm512_sub_epi64(_mm512_sub_epi64(d1,o4),z4);
 const __m512i a5=_mm512_add_epi64(d1,one),z5=_mm512_and_si512(_mm512_srlv_epi64(qb,a5),one),o5=_mm512_add_epi64(a5,z5),l5=_mm512_sub_epi64(_mm512_sub_epi64(d2,o5),z5);
 const __m512i a6=_mm512_add_epi64(d2,one),z6=_mm512_and_si512(_mm512_srlv_epi64(qb,a6),one),o6=_mm512_add_epi64(a6,z6),l6=_mm512_sub_epi64(_mm512_sub_epi64(L,o6),z6);
 #define VERT_G5(a) _mm512_i64gather_epi64(a,(const void*)0,1)
 // event_id exactly as event_pre_v3: first comma of the first 16 bytes, one quote pair stripped
 const __m512i idn=_mm512_sub_epi64(_mm512_sub_epi64(_mm512_min_epu64(c0,VERT_K5(59)),_mm512_add_epi64(z0,z0)),one);
 const __m512i S0=_mm512_add_epi64(P,z0);
 const __m512i lowbyte=VERT_K5(60);
 const __m512i S1=_mm512_add_epi64(P,o1);
 __m512i IDW,E0,U0;  // the ID's digits; words whose low byte is the ID's letter / the user's letter
 __mmask8 ukn;       // rows whose user letter is known
 if(OPT&1){
  const __m512i H0=_mm512_load_si512((const void*)s.h),H1=_mm512_load_si512((const void*)(s.h+8));
  const __m512i LO=_mm512_permutex2var_epi64(H0,_mm512_setr_epi64(0,2,4,6,8,10,12,14),H1),HI=_mm512_permutex2var_epi64(H0,_mm512_setr_epi64(1,3,5,7,9,11,13,15),H1); // bytes 0..7 / 8..15 of each line
  const __m512i z8=_mm512_slli_epi64(z0,3),i8=_mm512_add_epi64(z8,VERT_K5(44));
  E0=_mm512_srlv_epi64(LO,z8);
  IDW=_mm512_or_si512(_mm512_srlv_epi64(LO,i8),_mm512_sllv_epi64(HI,_mm512_sub_epi64(c64,i8))); // line bytes z0+1 .. z0+8
  // the user's letter is byte o1 of the line: in the saved bytes whenever the ID is a dense one (then o1 <= 13)
  U0=_mm512_srlv_epi64(_mm512_mask_blend_epi64(_mm512_cmple_epu64_mask(VERT_K5(44),o1),LO,HI),_mm512_slli_epi64(_mm512_and_si512(o1,VERT_K5(11)),3));
  ukn=_mm512_cmplt_epu64_mask(o1,VERT_K5(59));
 }else{
  E0=VERT_G5(S0);IDW=VERT_G5(_mm512_add_epi64(S0,one));U0=VERT_G5(S1);ukn=0xFF;
 }
 __mmask8 idok;const __m512i ve=id8k_v5(IDW,idn,c.ke,c.limE,idok);
 idok=_mm512_mask_cmpeq_epi64_mask(idok,_mm512_and_si512(E0,lowbyte),VERT_K5(61));
 // user: empty, or 'u' + 0..8 bytes kept as text
 const __m512i UW=VERT_G5(_mm512_add_epi64(S1,one));
 const __mmask8 hasu=_mm512_test_epi64_mask(l1,l1);
 ok=_kand_mask8(ok,_kor_mask8(_knot_mask8(hasu),_mm512_mask_cmple_epu64_mask(_mm512_mask_cmpeq_epi64_mask(ukn,_mm512_and_si512(U0,lowbyte),VERT_K5(62)),l1,VERT_K5(63))));
 const __m512i un=_mm512_maskz_sub_epi64(hasu,l1,one);
 const __m512i uw=_mm512_and_si512(UW,_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(un,3)))); // un == 0: shift by 64 gives 0
 // ingest_ts: 20 bytes, Z form
 const __m512i S3=_mm512_add_epi64(Bv,z3);
 const __m512i ig0=VERT_G5(S3),ig1=VERT_G5(_mm512_add_epi64(S3,VERT_K5(44))),ig2=VERT_G5(_mm512_add_epi64(S3,VERT_K5(64)));
 __mmask8 oki,hiti,oks,hits,okoff,okep,knz;
 const __m512i todi=tod8k_v5(ig1,ig2,oki);
 const __m512i dayi=day8k_v5(ig0,ig1,c.memo,hiti);
 const __m512i ingest=_mm512_add_epi64(dayi,todi);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_mask_cmpeq_epi64_mask(_kand_mask8(ok,_kand_mask8(oki,hiti)),_mm512_srli_epi64(ig2,56),VERT_K5(48)),l3,VERT_K5(51));
 // ts: 20 (Z), 13 (digits), 25 (offset)
 const __mmask8 is20=_mm512_cmpeq_epi64_mask(l2,VERT_K5(51)),is25=_mm512_cmpeq_epi64_mask(l2,VERT_K5(52)),is13=_mm512_cmpeq_epi64_mask(l2,VERT_K5(53));
 const __m512i S2=_mm512_add_epi64(P,o2);
 const __m512i ts0=VERT_G5(S2),ts1=VERT_G5(_mm512_add_epi64(S2,_mm512_mask_mov_epi64(VERT_K5(44),is13,VERT_K5(65))));
 const __m512i ts2=VERT_G5(_mm512_add_epi64(S2,VERT_K5(64))),ts3=VERT_G5(_mm512_add_epi64(S2,VERT_K5(66)));
 const __m512i tods=tod8k_v5(ts1,ts2,oks);
 __m512i days;
 if(OPT&2){
  const __m512i dd=_mm512_or_si512(_mm512_xor_si512(ts0,ig0),_mm512_and_si512(_mm512_xor_si512(ts1,ig1),VERT_K5(40)));
  if(_mm512_mask_test_epi64_mask(_kor_mask8(is20,is25),dd,dd)==0){days=dayi;hits=hiti;} // every Z/offset-form ts has its ingest date: same text, same entry
  else days=day8k_v5(ts0,ts1,c.memo,hits);
 }else days=day8k_v5(ts0,ts1,c.memo,hits);
 const __m512i off=off8k_v5(ts3,okoff);
 const __m512i ep=epoch8k_v5(ts0,ts1,okep);
 const __m512i b19=_mm512_srli_epi64(ts2,56);
 const __mmask8 z20=_mm512_mask_cmpeq_epi64_mask(is20,b19,VERT_K5(48)),p25=_mm512_mask_cmpeq_epi64_mask(is25,b19,VERT_K5(49)),m25=_mm512_mask_cmpeq_epi64_mask(is25,b19,VERT_K5(50));
 __m512i ts=_mm512_add_epi64(days,tods);
 ts=_mm512_mask_sub_epi64(ts,p25,ts,off);
 ts=_mm512_mask_add_epi64(ts,m25,ts,off);
 ts=_mm512_mask_mov_epi64(ts,is13,ep);
 // ts decided: 13 digits, or (valid time and known date) and (Z form, or offset form with a valid offset)
 ok=_kand_mask8(ok,_kor_mask8(_kand_mask8(is13,okep),_kand_mask8(_kand_mask8(oks,hits),_kor_mask8(z20,_kand_mask8(okoff,_kor_mask8(p25,m25))))));
 // kind, validity, k
 const __m512i kind=kind8k_v5(VERT_G5(_mm512_add_epi64(Bv,o4)),l4,knz);
 const __m512i tsrel=_mm512_sub_epi64(ts,VERT_C5(c.wstart));
 __mmask8 valid=_mm512_mask_cmplt_epu64_mask(_mm512_mask_test_epi64_mask(_kand_mask8(knz,hasu),l5,l5),tsrel,VERT_C5(c.wlen));
 valid=_mm512_mask_cmple_epi64_mask(valid,_mm512_sub_epi64(ingest,ts),VERT_K5(54));
 __m512i k=_mm512_slli_epi64(_mm512_add_epi64(tsrel,one),EV_TS_SHIFT);
 // purchases: the price as price_value takes it, for fields of up to 8 bytes (right-aligned over '0' like the ID digits)
 const __m512i S6=_mm512_add_epi64(Bv,o6);
 const __mmask8 pv=_mm512_mask_cmpeq_epi64_mask(valid,kind,VERT_K5(47));
 __mmask8 pur;
 {const __m512i n8=_mm512_slli_epi64(l6,3);
  const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(VERT_G5(S6),_mm512_sub_epi64(c64,n8)),_mm512_srlv_epi64(VERT_K5(1),n8));
  const __m512i v=_mm512_sub_epi8(x,VERT_K5(2)),nine=VERT_K5(4);
  const __m512i p4=_mm512_madd_epi16(_mm512_maddubs_epi16(v,VERT_K5(25)),VERT_K5(26));
  const __m512i price=_mm512_add_epi64(_mm512_mul_epu32(p4,VERT_K5(29)),_mm512_srli_epi64(p4,32));
  pur=_mm512_mask_cmple_epu64_mask(_mm512_mask_cmpeq_epi64_mask(pv,_mm512_max_epu8(v,nine),nine),_mm512_sub_epi64(price,one),VERT_K5(67)); // digits, 1..10000000 (an empty field gives 0)
  ok=_kandn_mask8(_mm512_mask_cmplt_epu64_mask(pv,VERT_K5(44),l6),ok);                                                             // a longer price field: the row code decides
  k=_mm512_mask_or_epi64(k,pur,k,_mm512_or_si512(price,VERT_K5(68)));
 }
 valid=_kand_mask8(valid,_kor_mask8(_knot_mask8(pv),pur)); // a purchase with a bad price is not a valid event
 const __m512i tid=VERT_C5(c.tid48);
 _mm512_store_si512((void*)o.ve,ve);_mm512_store_si512((void*)o.so,_mm512_slli_epi64(ve,5));
 _mm512_store_si512((void*)o.kk,_mm512_mask_mov_epi64(VERT_K5(69),valid,k));
 _mm512_store_si512((void*)o.ingest,ingest);
 _mm512_store_si512((void*)o.uct,_mm512_mask_or_epi64(tid,valid,tid,_mm512_or_si512(un,VERT_K5(70))));
 _mm512_store_si512((void*)o.uw,_mm512_maskz_mov_epi64(valid,uw));
 _mm512_store_si512((void*)o.s5,_mm512_add_epi64(Bv,o5));_mm512_store_si512((void*)o.l5,l5);
 _mm512_store_si512((void*)o.s6,S6);_mm512_store_si512((void*)o.l6,l6);
 ok=_kand_mask8(ok,idok);
 o.idok=(uint8_t)idok;o.ok=(uint8_t)ok;o.pur=(uint8_t)_kand_mask8(pur,ok);
 #undef VERT_G5
 #undef VERT_TZ5
}
VERT_T5 static inline void scan8_v5(const char* const* ls,Ev8Scan& s){scan8t_v5<VERT_EV8_OPT>(ls,s);}
VERT_T5 static inline void dec8_v5(const char* const* ls,const char* const* nx,const Ev8Scan& s,const EvConst& c,Ev8Out& o){dec8t_v5<VERT_EV8_OPT>(ls,nx,s,c,o);}
VERT_T5 static inline void ev8_v5(const char* const* ls,const char* const* nx,const EvConst& c,Ev8Out& o){
 Ev8Scan s;scan8_v5(ls,s);dec8_v5(ls,nx,s,c,o);
}
#undef VERT_C5
#undef VERT_LD5
#undef VERT_K5
} // namespace vert
static_assert(sizeof(vert::DateEnt)==sizeof(DateMemo3)&&offsetof(vert::DateEnt,k)==offsetof(DateMemo3,k)&&offsetof(vert::DateEnt,cc)==offsetof(DateMemo3,cc)&&offsetof(vert::DateEnt,ms)==offsetof(DateMemo3,ms));
static_assert(vert::ESLOT_INVALID_K==ESLOT_INVALID&&vert::EV_PURCHASE_BIT==EV_PURCHASE&&vert::RAW_USER==DENSE_USER_RAW&&sizeof(ESlot)==32);
static_assert(vert::MASK40==dn::MASK&&vert::I1==dn::I1&&vert::I2==dn::I2&&vert::EV_TS_SHIFT==EV_TS&&vert::DAYMS==DAY);
// Version 4 of the kernel (namespace vert4, /home/claude/vert/ev8_paste4.h pasted unchanged, default options): no gather
// instructions and no date memo; the dates it accepts are the year-months of the run's window (EvConst.d from
// make_dates). It is the default kernel of the ev8 loop; SS_EV8=3 picks version 3 above (same loop, same results).
// ---- ev8 VERSION 4: eight event lines per call (vertical AVX-512, no gather instructions, no date memo). Generated from
// ---- /home/claude/vert/vert.h by mkpaste4.py; contract in BATCH.md ("ev8 version 4"). Tested as this exact text by
// ---- vert_line_test4 -DVERT_PASTE. Do not edit here: change vert.h and regenerate.
// ---- Caller: vert4::EvConst ec{lp.ke, lp.limE, lp.wstart, lp.wlen, nullptr, lp.tid48}; ec.d = vert4::make_dates(window_start, window_end);
// ----         then vert4::scan8_v5 / dec8_v5 / ev8_v5 / ev8_ref exactly as the version 3 functions of namespace vert.
namespace vert4{
#ifndef VERT_T5
#define VERT_T5 __attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512cd,avx2,bmi,bmi2,lzcnt,popcnt")))
#endif
static constexpr uint64_t MASK40=(1ull<<40)-1;
static constexpr uint64_t M1=0x9E3779B97F4Bull&MASK40,M2=0xD6E8FEB86659ull&MASK40;
static constexpr uint64_t inv40(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&MASK40;}
static constexpr uint64_t I1=inv40(M1),I2=inv40(M2);
static_assert(((M1*I1)&MASK40)==1&&((M2*I2)&MASK40)==1);
static constexpr int64_t DAYMS=86400000LL;
static constexpr unsigned EV_TS_SHIFT=25; // main.cpp EV_TS
static constexpr uint64_t ESLOT_INVALID_K=2,EV_PURCHASE_BIT=1ull<<24,RAW_USER=0xC0000000u,PRICE_MAX=10000000; // main.cpp: ESLOT_INVALID, EV_PURCHASE, DENSE_USER_RAW, price_value's limit
static inline uint64_t index_of_ref(uint64_t y,uint64_t key){uint64_t m=(y*I2)&MASK40;m=m^(m>>17)^(m>>34);return ((m^key)*I1)&MASK40;}
static inline unsigned id8_ref(const uint64_t* w,const uint64_t* n,uint64_t key,uint64_t limit,uint64_t* ctr){
 unsigned acc=0;
 for(int i=0;i<8;++i){
  ctr[i]=0;
  const uint64_t len=n[i];
  if(len-1u>7u)continue;
  unsigned char s[8];memcpy(s,&w[i],8);
  if(s[0]=='0'&&len>1)continue;
  uint64_t v=0;bool ok=true;
  for(uint64_t j=0;j<len;++j){unsigned d=s[j]-(unsigned)'0';if(d>9){d=s[j]-(unsigned)'a';if(d>25){ok=false;break;}d+=10;}v=v*36+d;}
  if(!ok||v>MASK40)continue;
  const uint64_t c=index_of_ref(v,key);
  if(c<limit){ctr[i]=c;acc|=1u<<i;}
 }
 return acc;
}
static constexpr uint64_t VKT[]={
 /* 0*/ (uint64_t)(64),
 /* 1*/ (uint64_t)(uint8_t)(0x30)*0x0101010101010101ULL,
 /* 2*/ (uint64_t)(uint8_t)('0')*0x0101010101010101ULL,
 /* 3*/ (uint64_t)(uint8_t)('a')*0x0101010101010101ULL,
 /* 4*/ (uint64_t)(uint8_t)(9)*0x0101010101010101ULL,
 /* 5*/ (uint64_t)(uint8_t)(25)*0x0101010101010101ULL,
 /* 6*/ (uint64_t)(uint8_t)((char)(10-'a'))*0x0101010101010101ULL,
 /* 7*/ (uint64_t)(uint16_t)(0x0124)*0x0001000100010001ULL,
 /* 8*/ (uint64_t)(uint32_t)(0x00010510)*0x0000000100000001ULL,
 /* 9*/ (uint64_t)(1679616),
 /*10*/ (uint64_t)(1),
 /*11*/ (uint64_t)(7),
 /*12*/ (uint64_t)(MASK40),
 /*13*/ (uint64_t)(I2),
 /*14*/ (uint64_t)(I1),
 /*15*/ (uint64_t)(0x0000000030540000ULL),
 /*16*/ (uint64_t)(0xFFFFFFFF0200FFFFULL),
 /*17*/ (uint64_t)(0x0030303A30303A30ULL),
 /*18*/ (uint64_t)(0xFF09050009050009ULL),
 /*19*/ (uint64_t)(0x0000000064000000ULL),
 /*20*/ (uint64_t)(0x0000000001680000ULL),
 /*21*/ (uint64_t)(0x00010A00010A0001ULL),
 /*22*/ (uint64_t)(0x00010001003C0E10ULL),
 /*23*/ (uint64_t)(1000),
 /*24*/ (uint64_t)(86400000),
 /*25*/ (uint64_t)(0x010A010A010A010AULL),
 /*26*/ (uint64_t)(0x0001006400010064ULL),
 /*27*/ (uint64_t)(0x010A010A01000000ULL),
 /*28*/ (uint64_t)(0x0001006400010000ULL),
 /*29*/ (uint64_t)(10000),
 /*30*/ (uint64_t)(100000),
 /*31*/ (uint64_t)(0x30303A3030000000ULL),
 /*32*/ (uint64_t)(0x0905000902FFFFFFULL),
 /*33*/ (uint64_t)(0x010A00010A000000ULL),
 /*34*/ (uint64_t)(0x0001003C003C0000ULL),
 /*35*/ (uint64_t)(60000),
 /*36*/ (uint64_t)(0x40),
 /*37*/ (uint64_t)(0x3C),
 /*38*/ (uint64_t)(0x0F03),
 /*39*/ (uint64_t)(0xFFFFFFFFULL),
 /*40*/ (uint64_t)(0xFFFF),
 /*41*/ (uint64_t)(4),
 /*42*/ (uint64_t)(0x77656976ULL),
 /*43*/ (uint64_t)(0x74726163ULL),
 /*44*/ (uint64_t)(8),
 /*45*/ (uint64_t)(0x6573616863727570ULL),
 /*46*/ (uint64_t)(2),
 /*47*/ (uint64_t)(3),
 /*48*/ (uint64_t)('Z'),
 /*49*/ (uint64_t)('+'),
 /*50*/ (uint64_t)('-'),
 /*51*/ (uint64_t)(20),
 /*52*/ (uint64_t)(25),
 /*53*/ (uint64_t)(13),
 /*54*/ (uint64_t)(DAYMS),
 /*55*/ (uint64_t)(uint8_t)(',')*0x0101010101010101ULL,
 /*56*/ (uint64_t)(uint8_t)('"')*0x0101010101010101ULL,
 /*57*/ (uint64_t)(-1),
 /*58*/ (uint64_t)(63),
 /*59*/ (uint64_t)(16),
 /*60*/ (uint64_t)(0xFF),
 /*61*/ (uint64_t)('e'),
 /*62*/ (uint64_t)('u'),
 /*63*/ (uint64_t)(9),
 /*64*/ (uint64_t)(12),
 /*65*/ (uint64_t)(5),
 /*66*/ (uint64_t)(17),
 /*67*/ (uint64_t)(PRICE_MAX-1),
 /*68*/ (uint64_t)(EV_PURCHASE_BIT),
 /*69*/ (uint64_t)(ESLOT_INVALID_K),
 /*70*/ (uint64_t)(RAW_USER),
 /*71*/ (uint64_t)(0x3030),
 /*72*/ (uint64_t)(0xFFFFFFFFFFFF0909ULL),
 /*73*/ (uint64_t)(0x010A),
 /*74*/ (uint64_t)(128),
};
#ifndef VERT_K64
#define VERT_K64 0
#endif
#if VERT_K64
struct VKWide{alignas(64) uint64_t w[sizeof(VKT)/sizeof(VKT[0])*8];constexpr VKWide():w{}{for(unsigned i=0;i<sizeof(VKT)/sizeof(VKT[0]);++i)for(unsigned j=0;j<8;++j)w[8*i+j]=VKT[i];}};
static constexpr VKWide VKW{};
static inline __attribute__((always_inline)) const uint64_t* vk_ptr(){const uint64_t* p=VKW.w;__asm__("":"+r"(p));return p;}
#define VERT_K5(i) _mm512_load_si512((const void*)(VK+8*(i)))
#else
static inline __attribute__((always_inline)) const uint64_t* vk_ptr(){const uint64_t* p=VKT;__asm__("":"+r"(p));return p;}
#define VERT_K5(i) _mm512_set1_epi64((long long)VK[i])
#endif
VERT_T5 static inline __m512i id8k_v5(__m512i W,__m512i N,uint64_t key,uint64_t limit,__mmask8& okOut){
 const uint64_t* const VK=vk_ptr();
 const __m512i n8=_mm512_slli_epi64(N,3);
 const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(W,_mm512_sub_epi64(VERT_K5(0),n8)),_mm512_srlv_epi64(VERT_K5(1),n8));
 const __m512i t=_mm512_sub_epi8(x,VERT_K5(2)),u=_mm512_sub_epi8(x,VERT_K5(3));
 const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(t,VERT_K5(4)),_mm512_subs_epu8(u,VERT_K5(5)));
 const __m512i d=_mm512_min_epu8(t,_mm512_add_epi8(x,VERT_K5(6)));
 const __m512i p2=_mm512_maddubs_epi16(d,VERT_K5(7));
 const __m512i p4=_mm512_madd_epi16(p2,VERT_K5(8));
 const __m512i val=_mm512_add_epi64(_mm512_mul_epu32(p4,VERT_K5(9)),_mm512_srli_epi64(p4,32));
 // MIN36[n] picked by the low three bits of n (n == 8 -> entry 0); n == 0 and n > 8 are refused by the length test
 const __m512i min36=_mm512_permutexvar_epi64(N,_mm512_setr_epi64(78364164096LL,0,36,1296,46656,1679616,60466176,2176782336LL));
 __mmask8 ok=_mm512_testn_epi64_mask(bad,bad);
 // (each test is a compare under the mask of the ones before: the mask never leaves the mask registers)
 ok=_mm512_mask_cmple_epu64_mask(ok,_mm512_sub_epi64(N,VERT_K5(10)),VERT_K5(11));
 ok=_mm512_mask_cmpge_epu64_mask(ok,val,min36);
 ok=_mm512_mask_cmple_epu64_mask(ok,val,VERT_K5(12));
 const __m512i mask=VERT_K5(12);
 __m512i m=_mm512_and_si512(_mm512_mullo_epi64(val,VERT_K5(13)),mask);
 m=_mm512_ternarylogic_epi64(m,_mm512_srli_epi64(m,17),_mm512_srli_epi64(m,34),0x96);
 m=_mm512_xor_si512(m,_mm512_set1_epi64((long long)key));
 const __m512i c=_mm512_and_si512(_mm512_mullo_epi64(m,VERT_K5(14)),mask);
 okOut=_mm512_mask_cmplt_epu64_mask(ok,c,_mm512_set1_epi64((long long)limit));return c;
}
struct DateEnt{uint64_t k;uint32_t cc;uint32_t pad;int64_t ms;};
static_assert(sizeof(DateEnt)==24);
static constexpr unsigned ND=4; // version 4: at most this many year-months have dates the kernel accepts
struct EvDates{uint64_t pfx[ND];uint64_t base[ND];uint64_t dim[ND];uint64_t n;}; // "YYYY-MM-" as 8 bytes, ms of day 1 at 00:00Z, days of the month; n used (0: no date is accepted)
struct EvConst{uint64_t ke,limE;int64_t wstart;uint64_t wlen;const DateEnt* memo;uint64_t tid48=0;EvDates d={};}; // tid48 (= lp.tid48): ev8 only; d (= make_dates(window_start, window_end)): version 4 only, memo: versions 1-3 only
static inline unsigned char byte_of(uint64_t w,unsigned i){return (unsigned char)(w>>(8*i));}
static inline bool tod_ref(uint64_t w1,uint64_t w2,uint64_t& ms){
 if(byte_of(w1,2)!='T'||byte_of(w2,1)!=':'||byte_of(w2,4)!=':')return false;
 const unsigned ht=byte_of(w1,3)-(unsigned)'0',hu=byte_of(w2,0)-(unsigned)'0',mt=byte_of(w2,2)-(unsigned)'0',mu=byte_of(w2,3)-(unsigned)'0',st=byte_of(w2,5)-(unsigned)'0',su=byte_of(w2,6)-(unsigned)'0';
 if(ht>2||hu>9||mt>5||mu>9||st>5||su>9)return false;
 const uint64_t v=(uint64_t)(((ht*10+hu)*60+mt*10+mu)*60+st*10+su)*1000;
 if(v>=86400000ULL)return false;
 ms=v;return true;
}
static inline bool epoch_ref(uint64_t w0,uint64_t w1,uint64_t& ms){
 uint64_t hi=0,lo=0;
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w0,i)-(unsigned)'0';if(d>9)return false;hi=hi*10+d;}
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w1,i)-(unsigned)'0';if(d>9)return false;if(i>=3)lo=lo*10+d;}
 ms=hi*100000+lo;return true;
}
static inline bool off_ref(uint64_t w2,uint64_t w3,int64_t& delta){
 const unsigned char sg=byte_of(w2,7);
 if((sg!='+'&&sg!='-')||byte_of(w3,5)!=':')return false;
 const unsigned o1=byte_of(w3,3)-(unsigned)'0',o2=byte_of(w3,4)-(unsigned)'0',o3=byte_of(w3,6)-(unsigned)'0',o4=byte_of(w3,7)-(unsigned)'0';
 if(o1>9||o2>9||o3>9||o4>9)return false;
 const unsigned oh=o1*10+o2,om=o3*10+o4;
 if(oh>23||om>59)return false; // (ts_v3 says "not a timestamp" here; the kernels simply refuse the row)
 const int64_t off=(int64_t)(oh*60+om)*60000LL;
 delta=sg=='-'?off:-off;return true;
}
static inline unsigned kind_ref(uint64_t w,uint64_t n){
 if(n==4)return (uint32_t)w==0x77656976u?1u:((uint32_t)w==0x74726163u?2u:0u);
 if(n==8)return w==0x6573616863727570ULL?3u:0u;
 return 0;
}
#define VERT_C5(x) _mm512_set1_epi64((long long)(x))
#define VERT_LD5(p) _mm512_loadu_si512((const void*)(p))
VERT_T5 static inline __m512i tod8k_v5(__m512i a,__m512i b,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i va=_mm512_sub_epi8(a,VERT_K5(15)),la=VERT_K5(16);
 const __m512i vb=_mm512_sub_epi8(b,VERT_K5(17)),lb=VERT_K5(18);
 const __m512i pa=_mm512_madd_epi16(_mm512_maddubs_epi16(va,VERT_K5(19)),VERT_K5(20));
 const __m512i pb=_mm512_madd_epi16(_mm512_maddubs_epi16(vb,VERT_K5(21)),VERT_K5(22));
 const __m512i s=_mm512_add_epi32(pa,pb);
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(s,_mm512_srli_epi64(s,32)),VERT_K5(23));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(va,la),la),_mm512_max_epu8(vb,lb),lb),ms,VERT_K5(24));
 return ms;
}
VERT_T5 static inline __m512i epoch8k_v5(__m512i w0,__m512i w1,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i z=VERT_K5(2),nine=VERT_K5(4);
 const __m512i v0=_mm512_sub_epi8(w0,z),v1=_mm512_sub_epi8(w1,z);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v0,nine),nine),_mm512_max_epu8(v1,nine),nine);
 const __m512i p0=_mm512_madd_epi16(_mm512_maddubs_epi16(v0,VERT_K5(25)),VERT_K5(26));
 const __m512i p1=_mm512_madd_epi16(_mm512_maddubs_epi16(v1,VERT_K5(27)),VERT_K5(28));
 const __m512i tenk=VERT_K5(29);
 const __m512i hi=_mm512_add_epi64(_mm512_mul_epu32(p0,tenk),_mm512_srli_epi64(p0,32));
 const __m512i lo=_mm512_add_epi64(_mm512_mul_epu32(p1,tenk),_mm512_srli_epi64(p1,32));
 return _mm512_add_epi64(_mm512_mul_epu32(hi,VERT_K5(30)),lo);
}
VERT_T5 static inline __m512i off8k_v5(__m512i w3,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i v=_mm512_sub_epi8(w3,VERT_K5(31)),l=VERT_K5(32);
 const __m512i p=_mm512_madd_epi16(_mm512_maddubs_epi16(v,VERT_K5(33)),VERT_K5(34));
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(p,_mm512_srli_epi64(p,32)),VERT_K5(35));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v,l),l),ms,VERT_K5(24));
 return ms;
}
VERT_T5 static inline __m512i kind8k_v5(__m512i w,__m512i n,__mmask8& nz){
 const uint64_t* const VK=vk_ptr();
 const __m512i lo=_mm512_and_si512(w,VERT_K5(39));
 const __mmask8 is4=_mm512_cmpeq_epi64_mask(n,VERT_K5(41));
 const __mmask8 view=_mm512_mask_cmpeq_epi64_mask(is4,lo,VERT_K5(42)),cart=_mm512_mask_cmpeq_epi64_mask(is4,lo,VERT_K5(43));
 const __mmask8 pur=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(n,VERT_K5(44)),w,VERT_K5(45));
 nz=view|cart|pur;
 __m512i kind=_mm512_maskz_mov_epi64(view,VERT_K5(10));
 kind=_mm512_mask_mov_epi64(kind,cart,VERT_K5(46));
 return _mm512_mask_mov_epi64(kind,pur,VERT_K5(47));
}
struct alignas(64) Ev8Out{
 uint64_t ve[8];      // idok rows: event counter (== event_pre_v3)
 uint64_t so[8];      // idok rows: ve << 5 (byte offset of the 32-byte ESlot)
 uint64_t kk[8];      // ok rows: the slot's k: ESLOT_INVALID (2) if not a valid event, else (ts - window_start + 1) << 25,
                      //          for a purchase with EV_PURCHASE | price already in it (final)
 uint64_t ingest[8];  // ok rows: ingest_ts, ms
 uint64_t uct[8];     // ok rows: valid ? (DENSE_USER_RAW | n) | tid48 : tid48   (n = bytes after 'u', 0..8); the caller ORs cat << 32 for pur rows
 uint64_t uw[8];      // ok rows: valid ? the n bytes after 'u', zero-padded : 0
 const char* s5[8];const char* s6[8];uint64_t l5[8],l6[8]; // ok rows: product_id / price_cents content (quotes stripped); needed for pur rows (s5,l5)
 uint8_t ok;          // bit i: row i is on event_line_v3's fast path and all of the above is exact
 uint8_t idok;        // bit i: ve[i] is exactly event_pre_v3's counter of the line (ok implies idok)
 uint8_t pur;         // bit i: ok row that is a valid purchase (price already checked and in kk): the caller looks the category up
};
static inline uint64_t ld8_ref(const char* p){uint64_t w;memcpy(&w,p,8);return w;}
static inline void masks_ref(const char* p,uint64_t& c,uint64_t& q){c=0;q=0;for(int i=0;i<64;++i){c|=(uint64_t)(p[i]==',')<<i;q|=(uint64_t)(p[i]=='"')<<i;}}
static inline unsigned tz_ref(uint64_t x){return x?(unsigned)__builtin_ctzll(x):64u;}
static inline uint64_t blsr_ref(uint64_t x){return x&(x-1);}
struct alignas(64) Ev8Scan{uint64_t c1[8],q1[8],c2[8],q2[8],b[8],h[16];}; // h: the first 16 bytes of each line (OPT bit 0 only)
template<unsigned OPT> VERT_T5 static inline void scan8t_v5(const char* const* ls,Ev8Scan& s){
 const uint64_t* const VK=vk_ptr();
 const __m512i vc=VERT_K5(55),vq=VERT_K5(56);
 for(int i=0;i<8;++i){
  const char* const p=ls[i];
  const __m512i x=_mm512_loadu_si512((const void*)p);
  const uint64_t cm=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc));
  s.c1[i]=cm;s.q1[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq));
  if(OPT&1)_mm_storeu_si128((__m128i*)&s.h[2*i],_mm512_castsi512_si128(x));
  const char* const b=p+_tzcnt_u64(_blsr_u64(_blsr_u64(cm)))+1; // one past the third comma (p+65 if there is none)
  const __m512i y=_mm512_loadu_si512((const void*)b);
  s.c2[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vc));s.q2[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vq));s.b[i]=(uint64_t)b;
 }
}
static inline EvDates make_dates(int64_t window_start,int64_t window_end){
 EvDates r={};
 if(window_end<window_start)return r;
 const int64_t ingestBase=(int64_t)((uint64_t)window_start-(1ull<<38));
 const auto fl=[](int64_t a){return a>=0?a/DAYMS:-((-a+DAYMS-1)/DAYMS);};
 int64_t ly=-1;unsigned lm=0;
 for(int64_t day=fl(window_start)-3;day<=fl(window_end)+3&&r.n<ND;++day){
  const int64_t z=day+719468;const int64_t era=(z>=0?z:z-146096)/146097;const unsigned doe=(unsigned)(z-era*146097);
  const unsigned yoe=(doe-doe/1460+doe/36524-doe/146096)/365;int64_t y=(int64_t)yoe+era*400;
  const unsigned doy=doe-(365*yoe+yoe/4-yoe/100),mp=(5*doy+2)/153,d=doy-(153*mp+2)/5+1,m=mp<10?mp+3:mp-9;y+=m<=2;
  if(y<0||y>9999)continue;
  if(y==ly&&m==lm)continue;
  ly=y;lm=m;
  static constexpr unsigned mlen[13]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  const unsigned dim=mlen[m]+((m==2&&((y%4==0&&y%100!=0)||y%400==0))?1u:0u);
  const int64_t first=(day-(int64_t)(d-1))*DAYMS,last=first+(int64_t)(dim-1)*DAYMS; // day 1 and the last day of this month
  // date3's range rule must hold for every day of the month (it is monotone: first and last day decide)
  if(first<ingestBase||(uint64_t)last-(uint64_t)ingestBase>=((1ull<<39)-1)-(uint64_t)DAYMS)continue;
  unsigned char t[8];
  t[0]=(unsigned char)('0'+y/1000);t[1]=(unsigned char)('0'+y/100%10);t[2]=(unsigned char)('0'+y/10%10);t[3]=(unsigned char)('0'+y%10);t[4]='-';
  t[5]=(unsigned char)('0'+m/10);t[6]=(unsigned char)('0'+m%10);t[7]='-';
  uint64_t w;memcpy(&w,t,8);
  r.pfx[r.n]=w;r.base[r.n]=(uint64_t)first;r.dim[r.n]=dim;++r.n;
 }
 return r;
}
static inline bool day4_ref(uint64_t w0,uint64_t w1,const EvDates& d,uint64_t& dayms){
 const unsigned a=byte_of(w1,0)-(unsigned)'0',b=byte_of(w1,1)-(unsigned)'0';
 if(a>9||b>9)return false;
 const uint64_t t=(uint64_t)(a*10+b)-1; // day - 1 (day 0 wraps and fails the test below)
 for(uint64_t j=0;j<d.n&&j<ND;++j)if(w0==d.pfx[j]){if(t>=d.dim[j])return false;dayms=d.base[j]+t*(uint64_t)DAYMS;return true;}
 return false;
}
VERT_T5 static inline __m512i day4k_v5(__m512i w0,__m512i w1,const EvDates& d,__mmask8& hit){
 const uint64_t* const VK=vk_ptr();
 const __m512i vd=_mm512_sub_epi8(w1,VERT_K5(71)),lim=VERT_K5(72);
 const __m512i t=_mm512_sub_epi64(_mm512_maddubs_epi16(vd,VERT_K5(73)),VERT_K5(10)); // day - 1 (bytes 2..7 are multiplied by 0)
 __m512i base=_mm512_setzero_si512(),dim=_mm512_setzero_si512();                      // no prefix: dim 0, nothing is below it
 const uint64_t n=d.n<ND?d.n:ND;
 for(uint64_t j=0;j<n;++j){
  const __mmask8 m=_mm512_cmpeq_epi64_mask(w0,_mm512_set1_epi64((long long)d.pfx[j]));
  base=_mm512_mask_mov_epi64(base,m,_mm512_set1_epi64((long long)d.base[j]));
  dim=_mm512_mask_mov_epi64(dim,m,_mm512_set1_epi64((long long)d.dim[j]));
 }
 hit=_mm512_mask_cmplt_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(vd,lim),lim),t,dim); // two digits, 1 <= day <= days of the month
 return _mm512_add_epi64(base,_mm512_mul_epu32(t,VERT_K5(24)));
}
VERT_T5 static inline __attribute__((always_inline)) void ld16x8_v5(const uint64_t* a,long off,__m512i& lo,__m512i& hi){
 #define VERT_X5(i) _mm_loadu_si128((const __m128i*)((const char*)a[i]+off))
 __m512i e=_mm512_castsi128_si512(VERT_X5(0)),q=_mm512_castsi128_si512(VERT_X5(1));
 e=_mm512_inserti32x4(e,VERT_X5(2),1);q=_mm512_inserti32x4(q,VERT_X5(3),1);
 e=_mm512_inserti32x4(e,VERT_X5(4),2);q=_mm512_inserti32x4(q,VERT_X5(5),2);
 e=_mm512_inserti32x4(e,VERT_X5(6),3);q=_mm512_inserti32x4(q,VERT_X5(7),3);
 #undef VERT_X5
 lo=_mm512_unpacklo_epi64(e,q);hi=_mm512_unpackhi_epi64(e,q);
}
static inline void ev8_ref(const char* const* ls,const char* const* nx,const EvConst& c,Ev8Out& o){
 unsigned okm=0,idm=0,pum=0;
 for(int i=0;i<8;++i){
  o.ve[i]=o.so[i]=o.kk[i]=o.ingest[i]=o.uct[i]=o.uw[i]=o.l5[i]=o.l6[i]=0;o.s5[i]=o.s6[i]=ls[i];
  const char* const p=ls[i];const char* const nl=nx[i]-1;
  uint64_t cm,qa;masks_ref(p,cm,qa);
  // the counter as event_pre_v3 sees it: text before the first comma of the first 16 bytes, one quote pair stripped
  {const unsigned cp=tz_ref(cm)<16?tz_ref(cm):16;const unsigned z=(unsigned)(qa&1);
   const uint64_t n=(uint64_t)cp-2*z-1,w=ld8_ref(p+z+1);uint64_t ctr[8];const uint64_t ws[8]={w,w,w,w,w,w,w,w},ns[8]={n,n,n,n,n,n,n,n};
   if(p[z]=='e'&&(id8_ref(ws,ns,c.ke,c.limE,ctr)&1)){o.ve[i]=ctr[0];o.so[i]=ctr[0]<<5;idm|=1u<<i;}}
  // structure, as event_line_v3 (DREC form: the newline is known)
  const uint64_t cA=blsr_ref(cm),cB=blsr_ref(cA),cR=blsr_ref(cB);
  const uint64_t m1=cB^(cB-1);
  const uint64_t q1=qa&m1,st1=((cm^cB)<<1)|1,en1=cm^cR;
  const uint64_t sq1=q1&st1,op1=(sq1+~en1)&en1,err1=op1^((q1&~st1)<<1);
  const uint64_t e2=tz_ref(cB);
  const char* const b=p+e2+1;
  uint64_t c2,q2;masks_ref(b,c2,q2);
  const uint64_t L=(uint64_t)(nl-b),nbit=1ull<<(L&63),keep=nbit-1;
  const uint64_t d=c2&keep,qb=q2&keep;
  const uint64_t st2=(d<<1)|1,en2=d|nbit;
  const uint64_t sq2=qb&st2,op2=(sq2+~en2)&en2,err2=op2^((qb&~st2)<<1);
  if((err1|err2|(uint64_t)(__builtin_popcountll(d)^3)|((e2|L)>>6))!=0)continue;
  uint64_t B1=st1+sq1,E1=en1-(op1>>1),B2=st2+sq2,E2=en2-(op2>>1);
  const unsigned z0=(unsigned)(q1&1);
  const unsigned l0=tz_ref(E1)-z0;
  if(l0==0)continue;
  const unsigned z3=(unsigned)(qb&1);
  const char* const s3=b+z3;const unsigned l3=tz_ref(E2)-z3;
  B1=blsr_ref(B1);E1=blsr_ref(E1);
  const unsigned b1=tz_ref(B1),l1=tz_ref(E1)-b1;
  B1=blsr_ref(B1);E1=blsr_ref(E1);
  const unsigned b2=tz_ref(B1),l2=tz_ref(E1)-b2;
  B2=blsr_ref(B2);E2=blsr_ref(E2);
  const unsigned b4=tz_ref(B2),l4=tz_ref(E2)-b4;
  B2=blsr_ref(B2);E2=blsr_ref(E2);
  const unsigned b5=tz_ref(B2),l5=tz_ref(E2)-b5;
  B2=blsr_ref(B2);E2=blsr_ref(E2);
  const unsigned b6=tz_ref(B2),l6=tz_ref(E2)-b6;
  const char* const s1=p+b1;const char* const s2=p+b2;
  o.s5[i]=b+b5;o.l5[i]=l5;o.s6[i]=b+b6;o.l6[i]=l6;
  const unsigned kind=kind_ref(ld8_ref(b+b4),l4);
  if(!(idm>>i&1))continue;                                   // (then l0 is 2..9 and the ID is the line's first field)
  if(!(l1==0||(s1[0]=='u'&&l1<=9)))continue;                 // user: empty, or 'u' + 0..8 bytes
  const uint64_t un=l1?l1-1:0,uw=!l1?0:(un==8?ld8_ref(s1+1):(ld8_ref(s1+1)&((1ull<<(8*un))-1)));
  if(l3!=20)continue;
  const uint64_t ig0=ld8_ref(s3),ig1=ld8_ref(s3+8),ig2=ld8_ref(s3+12);
  uint64_t todi,dayi;
  if(!tod_ref(ig1,ig2,todi)||byte_of(ig2,7)!='Z'||!day4_ref(ig0,ig1,c.d,dayi))continue;
  const int64_t ingest=(int64_t)(dayi+todi);
  o.ingest[i]=(uint64_t)ingest;
  int64_t ts;
  if(l2==13){uint64_t v;if(!epoch_ref(ld8_ref(s2),ld8_ref(s2+5),v))continue;ts=(int64_t)v;}
  else if(l2==20||l2==25){
   const uint64_t t0=ld8_ref(s2),t1=ld8_ref(s2+8),t2=ld8_ref(s2+12);
   uint64_t tods,days;
   if(!tod_ref(t1,t2,tods)||!day4_ref(t0,t1,c.d,days))continue;
   ts=(int64_t)(days+tods);
   if(l2==20){if(byte_of(t2,7)!='Z')continue;}
   else{int64_t delta;if(!off_ref(t2,ld8_ref(s2+17),delta))continue;ts+=delta;}
  }else continue;
  const uint64_t tsrel=(uint64_t)ts-(uint64_t)c.wstart;
  uint64_t k=0;
  if(kind!=0&&l1!=0&&l5!=0&&tsrel<c.wlen&&ingest-ts<=DAYMS)k=(tsrel+1)<<EV_TS_SHIFT;
  if(k&&kind==3){ // price_value: digits only, 1..10000000 (longer than 8 bytes: left to the row code)
   if(l6>8)continue;
   uint64_t price=0;bool dig=l6!=0;
   for(unsigned j=0;j<l6;++j){const unsigned dd=(unsigned char)b[b6+j]-(unsigned)'0';if(dd>9)dig=false;price=price*10+dd;}
   if(dig&&price>=1&&price<=PRICE_MAX){k|=EV_PURCHASE_BIT|price;pum|=1u<<i;}else k=0;
  }
  okm|=1u<<i;
  o.kk[i]=k?k:ESLOT_INVALID_K;o.uct[i]=(k?(RAW_USER|un):0)|c.tid48;o.uw[i]=k?uw:0;
 }
 o.ok=(uint8_t)okm;o.idok=(uint8_t)idm;o.pur=(uint8_t)pum;
}
VERT_T5 static inline __attribute__((always_inline)) void ld16x8p_v5(const char* const* a,__m512i& lo,__m512i& hi){
 #define VERT_X5(i) _mm_loadu_si128((const __m128i*)a[i])
 __m512i e=_mm512_castsi128_si512(VERT_X5(0)),q=_mm512_castsi128_si512(VERT_X5(1));
 e=_mm512_inserti32x4(e,VERT_X5(2),1);q=_mm512_inserti32x4(q,VERT_X5(3),1);
 e=_mm512_inserti32x4(e,VERT_X5(4),2);q=_mm512_inserti32x4(q,VERT_X5(5),2);
 e=_mm512_inserti32x4(e,VERT_X5(6),3);q=_mm512_inserti32x4(q,VERT_X5(7),3);
 #undef VERT_X5
 lo=_mm512_unpacklo_epi64(e,q);hi=_mm512_unpackhi_epi64(e,q);
}
#ifndef VERT_EV4_OPT
#define VERT_EV4_OPT 0
#endif
template<unsigned OPT> VERT_T5 static inline void dec4t_v5(const char* const* ls,const char* const* nx,const Ev8Scan& s,const EvConst& c,Ev8Out& o){
 const uint64_t* const VK=vk_ptr();
 const uint64_t* const aC1=s.c1;const uint64_t* const aQ1=s.q1;const uint64_t* const aC2=s.c2;const uint64_t* const aQ2=s.q2;const uint64_t* const aB=s.b;
 // ---- vertical (the text of dec8t_v5) ----
 const __m512i one=VERT_K5(10),ones=VERT_K5(57),c63=VERT_K5(58),c64=VERT_K5(0);
 #define VERT_TZ5(x,xm) _mm512_sub_epi64(c63,_mm512_lzcnt_epi64(_mm512_xor_si512(x,xm))) /* tzcnt(x) given xm = x-1 (63 for x == 0) */
 const __m512i P=VERT_LD5(ls),NL=_mm512_add_epi64(VERT_LD5(nx),ones),Bv=_mm512_load_si512((const void*)aB);
 const __m512i C1=_mm512_load_si512((const void*)aC1),Q1=_mm512_load_si512((const void*)aQ1),C2=_mm512_load_si512((const void*)aC2),Q2=_mm512_load_si512((const void*)aQ2);
 const __m512i t0=_mm512_add_epi64(C1,ones),c0=VERT_TZ5(C1,t0),cA=_mm512_and_si512(C1,t0);
 const __m512i t1=_mm512_add_epi64(cA,ones),c1=VERT_TZ5(cA,t1),cB=_mm512_and_si512(cA,t1);
 const __m512i t2=_mm512_add_epi64(cB,ones),m1=_mm512_xor_si512(cB,t2),cR=_mm512_and_si512(cB,t2);
 const __m512i c2=_mm512_add_epi64(_mm512_sub_epi64(Bv,P),ones); // tzcnt(cB), 64 if there is no third comma
 const __m512i q1=_mm512_and_si512(Q1,m1),st1=_mm512_or_si512(_mm512_slli_epi64(_mm512_xor_si512(C1,cB),1),one),en1=_mm512_xor_si512(C1,cR);
 const __m512i sq1=_mm512_and_si512(q1,st1),op1=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq1,en1),ones),en1);
 const __m512i err1=_mm512_xor_si512(op1,_mm512_slli_epi64(_mm512_andnot_si512(st1,q1),1));
 const __m512i L=_mm512_sub_epi64(NL,Bv),nbit=_mm512_sllv_epi64(one,_mm512_and_si512(L,c63)),keep=_mm512_add_epi64(nbit,ones);
 const __m512i d=_mm512_and_si512(C2,keep),qb=_mm512_and_si512(Q2,keep);
 const __m512i st2=_mm512_or_si512(_mm512_slli_epi64(d,1),one),en2=_mm512_or_si512(d,nbit);
 const __m512i sq2=_mm512_and_si512(qb,st2),op2=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq2,en2),ones),en2);
 const __m512i err2=_mm512_xor_si512(op2,_mm512_slli_epi64(_mm512_andnot_si512(st2,qb),1));
 const __m512i u0=_mm512_add_epi64(d,ones),d0=VERT_TZ5(d,u0),dA=_mm512_and_si512(d,u0);
 const __m512i u1=_mm512_add_epi64(dA,ones),d1=VERT_TZ5(dA,u1),dB=_mm512_and_si512(dA,u1);
 const __m512i u2=_mm512_add_epi64(dB,ones),d2=VERT_TZ5(dB,u2),dR=_mm512_and_si512(dB,u2);
 const __m512i err=_mm512_ternarylogic_epi64(err1,err2,dR,0xFE);            // a quote error, or a fourth comma before the newline
 __mmask8 ok=_mm512_testn_epi64_mask(err,err);
 ok=_mm512_mask_test_epi64_mask(ok,dB,dB);                                    // at least three commas
 ok=_mm512_mask_cmplt_epu64_mask(ok,_mm512_or_si512(c2,L),c64);               // third comma and newline inside their windows ((e2|L)>>6 == 0)
 const __m512i z0=_mm512_and_si512(Q1,one);
 const __m512i a1=_mm512_add_epi64(c0,one),z1=_mm512_and_si512(_mm512_srlv_epi64(q1,a1),one),o1=_mm512_add_epi64(a1,z1),l1=_mm512_sub_epi64(_mm512_sub_epi64(c1,o1),z1);
 const __m512i a2=_mm512_add_epi64(c1,one),z2=_mm512_and_si512(_mm512_srlv_epi64(q1,a2),one),o2=_mm512_add_epi64(a2,z2),l2=_mm512_sub_epi64(_mm512_sub_epi64(c2,o2),z2);
 const __m512i z3=_mm512_and_si512(qb,one),l3=_mm512_sub_epi64(d0,_mm512_add_epi64(z3,z3));
 const __m512i a4=_mm512_add_epi64(d0,one),z4=_mm512_and_si512(_mm512_srlv_epi64(qb,a4),one),o4=_mm512_add_epi64(a4,z4),l4=_mm512_sub_epi64(_mm512_sub_epi64(d1,o4),z4);
 const __m512i a5=_mm512_add_epi64(d1,one),z5=_mm512_and_si512(_mm512_srlv_epi64(qb,a5),one),o5=_mm512_add_epi64(a5,z5),l5=_mm512_sub_epi64(_mm512_sub_epi64(d2,o5),z5);
 const __m512i a6=_mm512_add_epi64(d2,one),z6=_mm512_and_si512(_mm512_srlv_epi64(qb,a6),one),o6=_mm512_add_epi64(a6,z6),l6=_mm512_sub_epi64(_mm512_sub_epi64(L,o6),z6);
 // ---- operand fetch without gathers ----
 // The five field addresses per row go through memory into general registers. The kind and price addresses are
 // limited to line start + 128: an ok row has both at or below line start + 127 (third comma within 64 bytes, newline
 // less than 64 bytes after it), so only rows that are not ok are changed and every 16-byte load stays inside the
 // 144 readable bytes (the largest other reach is ts + 28 <= line start + 93).
 const __m512i S1=_mm512_add_epi64(P,o1),S2=_mm512_add_epi64(P,o2),S3=_mm512_add_epi64(Bv,z3),S6=_mm512_add_epi64(Bv,o6);
 const __m512i top=_mm512_add_epi64(P,VERT_K5(74));
 alignas(64) uint64_t AD[40];
 _mm512_store_si512((void*)AD,S1);_mm512_store_si512((void*)(AD+8),S2);_mm512_store_si512((void*)(AD+16),S3);
 _mm512_store_si512((void*)(AD+24),_mm512_min_epu64(_mm512_add_epi64(Bv,o4),top));_mm512_store_si512((void*)(AD+32),_mm512_min_epu64(S6,top));
 const uint64_t* ad=AD;__asm__("":"+r"(ad)::"memory"); // the reads below are scalar loads from memory, not extracts from the vectors
 __m512i U0,U1,ts0,tsh,ts2,tsx,ig0,ig1,igl,ig2,KW,kh,PW,ph;
 ld16x8_v5(ad,0,U0,U1);          // user: letter + the 15 bytes after it
 ld16x8_v5(ad+8,0,ts0,tsh);      // ts bytes 0..15
 ld16x8_v5(ad+8,12,ts2,tsx);     // ts bytes 12..27
 ld16x8_v5(ad+16,0,ig0,ig1);     // ingest_ts bytes 0..15
 ld16x8_v5(ad+16,4,igl,ig2);     // ingest_ts bytes 4..19 (the upper half is bytes 12..19)
 ld16x8_v5(ad+24,0,KW,kh);       // event_type
 ld16x8_v5(ad+32,0,PW,ph);       // price_cents
 (void)igl;(void)kh;(void)ph;
 // ---- decode (the text of dec8t_v5<1> with the operands above; dates by day4k_v5) ----
 // event_id exactly as event_pre_v3: first comma of the first 16 bytes, one quote pair stripped; from the head bytes the scan saved
 const __m512i idn=_mm512_sub_epi64(_mm512_sub_epi64(_mm512_min_epu64(c0,VERT_K5(59)),_mm512_add_epi64(z0,z0)),one);
 const __m512i lowbyte=VERT_K5(60);
 __m512i LO,HI; // bytes 0..7 / 8..15 of each line
 if(OPT&1)ld16x8p_v5(ls,LO,HI);
 else{
  const __m512i H0=_mm512_load_si512((const void*)s.h),H1=_mm512_load_si512((const void*)(s.h+8));
  LO=_mm512_permutex2var_epi64(H0,_mm512_setr_epi64(0,2,4,6,8,10,12,14),H1);HI=_mm512_permutex2var_epi64(H0,_mm512_setr_epi64(1,3,5,7,9,11,13,15),H1);
 }
 const __m512i z8=_mm512_slli_epi64(z0,3),i8=_mm512_add_epi64(z8,VERT_K5(44));
 const __m512i E0=_mm512_srlv_epi64(LO,z8);
 const __m512i IDW=_mm512_or_si512(_mm512_srlv_epi64(LO,i8),_mm512_sllv_epi64(HI,_mm512_sub_epi64(c64,i8))); // line bytes z0+1 .. z0+8
 __mmask8 idok;const __m512i ve=id8k_v5(IDW,idn,c.ke,c.limE,idok);
 idok=_mm512_mask_cmpeq_epi64_mask(idok,_mm512_and_si512(E0,lowbyte),VERT_K5(61));
 // user: empty, or 'u' + 0..8 bytes kept as text
 const __m512i UW=_mm512_or_si512(_mm512_srli_epi64(U0,8),_mm512_slli_epi64(U1,56)); // the 8 bytes after the letter
 const __mmask8 hasu=_mm512_test_epi64_mask(l1,l1);
 ok=_kand_mask8(ok,_kor_mask8(_knot_mask8(hasu),_mm512_mask_cmple_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_and_si512(U0,lowbyte),VERT_K5(62)),l1,VERT_K5(63))));
 const __m512i un=_mm512_maskz_sub_epi64(hasu,l1,one);
 const __m512i uw=_mm512_and_si512(UW,_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(un,3)))); // un == 0: shift by 64 gives 0
 // ingest_ts: 20 bytes, Z form
 __mmask8 oki,hiti,oks,hits,okoff,okep,knz;
 const __m512i todi=tod8k_v5(ig1,ig2,oki);
 const __m512i dayi=day4k_v5(ig0,ig1,c.d,hiti);
 const __m512i ingest=_mm512_add_epi64(dayi,todi);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_mask_cmpeq_epi64_mask(_kand_mask8(ok,_kand_mask8(oki,hiti)),_mm512_srli_epi64(ig2,56),VERT_K5(48)),l3,VERT_K5(51));
 // ts: 20 (Z), 13 (digits), 25 (offset)
 const __mmask8 is20=_mm512_cmpeq_epi64_mask(l2,VERT_K5(51)),is25=_mm512_cmpeq_epi64_mask(l2,VERT_K5(52)),is13=_mm512_cmpeq_epi64_mask(l2,VERT_K5(53));
 const __m512i ts1=_mm512_mask_or_epi64(tsh,is13,_mm512_srli_epi64(ts0,40),_mm512_slli_epi64(tsh,24)); // bytes 8..15, for 13 digits bytes 5..12
 const __m512i ts3=_mm512_or_si512(_mm512_srli_epi64(ts2,40),_mm512_slli_epi64(tsx,24));               // bytes 17..24
 const __m512i tods=tod8k_v5(ts1,ts2,oks);
 __m512i days;
 if(OPT&2){
  const __m512i dd=_mm512_or_si512(_mm512_xor_si512(ts0,ig0),_mm512_and_si512(_mm512_xor_si512(ts1,ig1),VERT_K5(40)));
  if(_mm512_mask_test_epi64_mask(_kor_mask8(is20,is25),dd,dd)==0){days=dayi;hits=hiti;} // every Z/offset-form ts has its ingest date: same text, same answer
  else days=day4k_v5(ts0,ts1,c.d,hits);
 }else days=day4k_v5(ts0,ts1,c.d,hits);
 const __m512i off=off8k_v5(ts3,okoff);
 const __m512i ep=epoch8k_v5(ts0,ts1,okep);
 const __m512i b19=_mm512_srli_epi64(ts2,56);
 const __mmask8 z20=_mm512_mask_cmpeq_epi64_mask(is20,b19,VERT_K5(48)),p25=_mm512_mask_cmpeq_epi64_mask(is25,b19,VERT_K5(49)),m25=_mm512_mask_cmpeq_epi64_mask(is25,b19,VERT_K5(50));
 __m512i ts=_mm512_add_epi64(days,tods);
 ts=_mm512_mask_sub_epi64(ts,p25,ts,off);
 ts=_mm512_mask_add_epi64(ts,m25,ts,off);
 ts=_mm512_mask_mov_epi64(ts,is13,ep);
 // ts decided: 13 digits, or (valid time and known date) and (Z form, or offset form with a valid offset)
 ok=_kand_mask8(ok,_kor_mask8(_kand_mask8(is13,okep),_kand_mask8(_kand_mask8(oks,hits),_kor_mask8(z20,_kand_mask8(okoff,_kor_mask8(p25,m25))))));
 // kind, validity, k
 const __m512i kind=kind8k_v5(KW,l4,knz);
 const __m512i tsrel=_mm512_sub_epi64(ts,VERT_C5(c.wstart));
 __mmask8 valid=_mm512_mask_cmplt_epu64_mask(_mm512_mask_test_epi64_mask(_kand_mask8(knz,hasu),l5,l5),tsrel,VERT_C5(c.wlen));
 valid=_mm512_mask_cmple_epi64_mask(valid,_mm512_sub_epi64(ingest,ts),VERT_K5(54));
 __m512i k=_mm512_slli_epi64(_mm512_add_epi64(tsrel,one),EV_TS_SHIFT);
 // purchases: the price as price_value takes it, for fields of up to 8 bytes (right-aligned over '0' like the ID digits)
 const __mmask8 pv=_mm512_mask_cmpeq_epi64_mask(valid,kind,VERT_K5(47));
 __mmask8 pur;
 {const __m512i n8=_mm512_slli_epi64(l6,3);
  const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(PW,_mm512_sub_epi64(c64,n8)),_mm512_srlv_epi64(VERT_K5(1),n8));
  const __m512i v=_mm512_sub_epi8(x,VERT_K5(2)),nine=VERT_K5(4);
  const __m512i p4=_mm512_madd_epi16(_mm512_maddubs_epi16(v,VERT_K5(25)),VERT_K5(26));
  const __m512i price=_mm512_add_epi64(_mm512_mul_epu32(p4,VERT_K5(29)),_mm512_srli_epi64(p4,32));
  pur=_mm512_mask_cmple_epu64_mask(_mm512_mask_cmpeq_epi64_mask(pv,_mm512_max_epu8(v,nine),nine),_mm512_sub_epi64(price,one),VERT_K5(67)); // digits, 1..10000000 (an empty field gives 0)
  ok=_kandn_mask8(_mm512_mask_cmplt_epu64_mask(pv,VERT_K5(44),l6),ok);                                                             // a longer price field: the row code decides
  k=_mm512_mask_or_epi64(k,pur,k,_mm512_or_si512(price,VERT_K5(68)));
 }
 valid=_kand_mask8(valid,_kor_mask8(_knot_mask8(pv),pur)); // a purchase with a bad price is not a valid event
 const __m512i tid=VERT_C5(c.tid48);
 _mm512_store_si512((void*)o.ve,ve);_mm512_store_si512((void*)o.so,_mm512_slli_epi64(ve,5));
 _mm512_store_si512((void*)o.kk,_mm512_mask_mov_epi64(VERT_K5(69),valid,k));
 _mm512_store_si512((void*)o.ingest,ingest);
 _mm512_store_si512((void*)o.uct,_mm512_mask_or_epi64(tid,valid,tid,_mm512_or_si512(un,VERT_K5(70))));
 _mm512_store_si512((void*)o.uw,_mm512_maskz_mov_epi64(valid,uw));
 _mm512_store_si512((void*)o.s5,_mm512_add_epi64(Bv,o5));_mm512_store_si512((void*)o.l5,l5);
 _mm512_store_si512((void*)o.s6,S6);_mm512_store_si512((void*)o.l6,l6);
 ok=_kand_mask8(ok,idok);
 o.idok=(uint8_t)idok;o.ok=(uint8_t)ok;o.pur=(uint8_t)_kand_mask8(pur,ok);
 #undef VERT_TZ5
}
VERT_T5 static inline void scan8_v5(const char* const* ls,Ev8Scan& s){scan8t_v5<(VERT_EV4_OPT&1)?0u:1u>(ls,s);}
VERT_T5 static inline void dec8_v5(const char* const* ls,const char* const* nx,const Ev8Scan& s,const EvConst& c,Ev8Out& o){dec4t_v5<VERT_EV4_OPT>(ls,nx,s,c,o);}
VERT_T5 static inline void ev8_v5(const char* const* ls,const char* const* nx,const EvConst& c,Ev8Out& o){
 Ev8Scan s;scan8_v5(ls,s);dec8_v5(ls,nx,s,c,o);
}
#undef VERT_C5
#undef VERT_LD5
#undef VERT_K5
} // namespace vert4
static_assert(vert4::ESLOT_INVALID_K==ESLOT_INVALID&&vert4::EV_PURCHASE_BIT==EV_PURCHASE&&vert4::RAW_USER==DENSE_USER_RAW&&sizeof(ESlot)==32);
static_assert(vert4::MASK40==dn::MASK&&vert4::I1==dn::I1&&vert4::I2==dn::I2&&vert4::EV_TS_SHIFT==EV_TS&&vert4::DAYMS==DAY);
// ==== /home/claude/vert/ev8g.h pasted unchanged (agent P3's kernel for opaque IDs; contract: vert/BATCH.md "ev8g") ====
// ---- ev8g: eight event lines per call with OPAQUE event_id / user_id (GFAST.md package A; agent P3, 2026-11-21).
// ---- /home/claude/vert/ev8g.h; contract in BATCH.md ("ev8g"); tested as this exact text by ev8g_test.cpp.
// ---- Paste / include AFTER ev8_paste4.h: it calls the proven version 4 pieces (vert4::tod8k_v5, day4k_v5, epoch8k_v5,
// ---- off8k_v5, kind8k_v5, ld16x8_v5 and their scalar twins) and uses vert4::EvConst (wstart, wlen, d; ke / limE /
// ---- tid48 / memo are not read). AVX-512 F/BW/DQ/VL/CD, no gather instruction, zmm only.
// A line is cut in three 64-byte windows instead of two: A at the line start (event_id up to its comma), B behind
// that comma (user_id and ts up to the third comma of the line), C behind the third comma (ingest_ts, event_type,
// product_id, price_cents up to the newline): exactly version 4's second window. So each of the three parts may be
// up to 63 bytes long (a quoted 33-byte ID is 35), a line up to 191; version 4 wanted the first three fields in 64.
// 224 readable bytes from every ls[i] (the mappings leave 4096 zero bytes behind each file).
namespace vertg{
using vert4::EvConst;using vert4::EvDates;
static constexpr uint64_t CLS_SHORT=0,CLS_LONG=1,CLS_OTHER=2; // P1's GRec classes
struct alignas(64) Ev8gScan{uint64_t cA[8],qA[8],cB[8],qB[8],cC[8],qC[8],bB[8],bC[8];};
struct alignas(64) Ev8gOut{
 // all fields: ok rows only
 uint64_t ecls[8];    // event_id: 0 = 'e' + 0..8 bytes of [0-9a-z]; 1 = 'e' + exactly 32 of [0-9a-f]; 2 = any other text (not empty)
 uint64_t ekey[8];    // class 0: the bytes after the 'e' as a word (first byte lowest, zero padded); 1: the last 16 hex digits as a number; 2: pointer to the text (= es)
 uint64_t ehi[8];     // class 1: the first 16 hex digits as a number; else 0
 const char* es[8];uint64_t el[8]; // event_id text (quotes stripped) and its length (1..63)
 uint64_t ucls[8];    // user_id: 0 = at most 11 bytes (any bytes; 0 bytes = empty); 1 = 'u' + exactly 32 of [0-9a-f]; 2 = any other text
 uint64_t uk0[8],uk1[8]; // class 0: KeyTable::pack of the text (k0 = bytes 0..7, k1 = bytes 8..10 | (length + 1) << 24); else 0
 uint64_t ulo[8],uhi[8]; // class 1: as ekey / ehi; else 0
 const char* us[8];uint64_t ul[8]; // user_id text (quotes stripped) and its length (0..63)
 uint64_t kk[8];      // PRec::k: 0 if the line is not a valid event, else (ts - window_start + 1) << 25, for a purchase | EV_PURCHASE | price (final)
 uint64_t ingest[8];  // ingest_ts, ms
 const char* s5[8];const char* s6[8];uint64_t l5[8],l6[8]; // product_id / price_cents content (quotes stripped)
 uint8_t ok;          // bit i: every field above is exact for row i (what the scalar line code computes)
 uint8_t pur;         // bit i: ok row that is a valid purchase: the caller looks the category of (s5, l5) up
};
// ---------------- scalar twin: the same decisions, written row by row ----------------
static inline uint64_t hex16_ref(const char* s,bool& good){uint64_t v=0;for(int i=0;i<16;++i){const unsigned ch=(unsigned char)s[i];unsigned d;
  if(ch>='0'&&ch<='9')d=ch-'0';else if(ch>='a'&&ch<='f')d=ch-'a'+10;else{good=false;d=0;}v=(v<<4)|d;}return v;}
static inline void ev8g_ref(const char* const* ls,const char* const* nx,const EvConst& c,Ev8gOut& o){
 using namespace vert4;
 unsigned okm=0,pum=0;
 for(int i=0;i<8;++i){
  o.ecls[i]=o.ekey[i]=o.ehi[i]=o.el[i]=o.ucls[i]=o.uk0[i]=o.uk1[i]=o.ulo[i]=o.uhi[i]=o.ul[i]=o.kk[i]=o.ingest[i]=o.l5[i]=o.l6[i]=0;o.es[i]=o.us[i]=o.s5[i]=o.s6[i]=ls[i];
  const char* const p=ls[i];const char* const nl=nx[i]-1;
  uint64_t cA,qA;masks_ref(p,cA,qA);
  if(!cA)continue;                                              // no comma in window A
  const unsigned c0=tz_ref(cA);const uint64_t mA=cA^(cA-1),enA=cA&~blsr_ref(cA);
  const uint64_t qa=qA&mA,sqA=qa&1,opA=(sqA+~enA)&enA,errA=opA^((qa&~1ull)<<1);
  const char* const bB=p+c0+1;
  uint64_t cB,qB;masks_ref(bB,cB,qB);
  const uint64_t cX=blsr_ref(cB),cY=blsr_ref(cX);
  if(!cX)continue;                                              // fewer than two commas in window B
  const unsigned e1=tz_ref(cB),e2=tz_ref(cX);const uint64_t mB=cX^(cX-1);
  const uint64_t q1=qB&mB,st1=((cB^cX)<<1)|1,en1=cB^cY;
  const uint64_t sq1=q1&st1,op1=(sq1+~en1)&en1,err1=op1^((q1&~st1)<<1);
  const char* const b=bB+e2+1;
  uint64_t c2,q2;masks_ref(b,c2,q2);
  const uint64_t L=(uint64_t)(nl-b);
  if(L>>6)continue;                                             // newline not inside window C
  const uint64_t nbit=1ull<<L,keep=nbit-1;
  const uint64_t d=c2&keep,qb=q2&keep;
  const uint64_t st2=(d<<1)|1,en2=d|nbit;
  const uint64_t sq2=qb&st2,op2=(sq2+~en2)&en2,err2=op2^((qb&~st2)<<1);
  if((errA|err1|err2|(uint64_t)(__builtin_popcountll(d)^3))!=0)continue;
  const unsigned z0=(unsigned)(qA&1),l0=c0-2*z0;
  if(l0==0)continue;                                            // empty event_id: the line code drops the line
  const unsigned z1=(unsigned)(qB&1),l1=e1-2*z1;
  const unsigned z2=(unsigned)((q1>>(e1+1))&1),o2=e1+1+z2,l2=e2-o2-z2;
  uint64_t B2=st2+sq2,E2=en2-(op2>>1);
  const unsigned z3=(unsigned)(qb&1);
  const char* const s3=b+z3;const unsigned l3=tz_ref(E2)-z3;
  B2=blsr_ref(B2);E2=blsr_ref(E2);const unsigned b4=tz_ref(B2),l4=tz_ref(E2)-b4;
  B2=blsr_ref(B2);E2=blsr_ref(E2);const unsigned b5=tz_ref(B2),l5=tz_ref(E2)-b5;
  B2=blsr_ref(B2);E2=blsr_ref(E2);const unsigned b6=tz_ref(B2),l6=tz_ref(E2)-b6;
  const char* const s0=p+z0;const char* const s1=bB+z1;const char* const s2=bB+o2;
  const unsigned kind=kind_ref(ld8_ref(b+b4),l4);
  if(l3!=20)continue;
  const uint64_t ig0=ld8_ref(s3),ig1=ld8_ref(s3+8),ig2=ld8_ref(s3+12);
  uint64_t todi,dayi;
  if(!tod_ref(ig1,ig2,todi)||byte_of(ig2,7)!='Z'||!day4_ref(ig0,ig1,c.d,dayi))continue;
  const int64_t ingest=(int64_t)(dayi+todi);
  int64_t ts;
  if(l2==13){uint64_t v;if(!epoch_ref(ld8_ref(s2),ld8_ref(s2+5),v))continue;ts=(int64_t)v;}
  else if(l2==20||l2==25){
   const uint64_t t0=ld8_ref(s2),t1=ld8_ref(s2+8),t2=ld8_ref(s2+12);
   uint64_t tods,days;
   if(!tod_ref(t1,t2,tods)||!day4_ref(t0,t1,c.d,days))continue;
   ts=(int64_t)(days+tods);
   if(l2==20){if(byte_of(t2,7)!='Z')continue;}
   else{int64_t delta;if(!off_ref(t2,ld8_ref(s2+17),delta))continue;ts+=delta;}
  }else continue;
  const uint64_t tsrel=(uint64_t)ts-(uint64_t)c.wstart;
  uint64_t k=0;
  if(kind!=0&&l1!=0&&l5!=0&&tsrel<c.wlen&&ingest-ts<=DAYMS)k=(tsrel+1)<<EV_TS_SHIFT;
  bool pur=false;
  if(k&&kind==3){
   if(l6>8)continue;                                            // a longer price field: the line code decides
   uint64_t price=0;bool dig=l6!=0;
   for(unsigned j=0;j<l6;++j){const unsigned dd=(unsigned char)b[b6+j]-(unsigned)'0';if(dd>9)dig=false;price=price*10+dd;}
   if(dig&&price>=1&&price<=PRICE_MAX){k|=EV_PURCHASE_BIT|price;pur=true;}else k=0;
  }
  okm|=1u<<i;if(pur)pum|=1u<<i;
  o.kk[i]=k;o.ingest[i]=(uint64_t)ingest;o.s5[i]=b+b5;o.l5[i]=l5;o.s6[i]=b+b6;o.l6[i]=l6;
  // the two IDs, by a plain reading of the text
  o.es[i]=s0;o.el[i]=l0;o.us[i]=s1;o.ul[i]=l1;
  {uint64_t cls=CLS_OTHER,key=(uint64_t)s0,hi=0;
   if(s0[0]=='e'&&l0<=9){bool g=true;uint64_t w=0;for(unsigned j=1;j<l0;++j){const unsigned ch=(unsigned char)s0[j];if(!((ch>='0'&&ch<='9')||(ch>='a'&&ch<='z')))g=false;w|=(uint64_t)ch<<(8*(j-1));}
    if(g){cls=CLS_SHORT;key=w;}}
   else if(s0[0]=='e'&&l0==33){bool g=true;const uint64_t h=hex16_ref(s0+1,g),lo=hex16_ref(s0+17,g);if(g){cls=CLS_LONG;key=lo;hi=h;}}
   o.ecls[i]=cls;o.ekey[i]=key;o.ehi[i]=hi;}
  {uint64_t cls=CLS_OTHER,k0=0,k1=0,lo=0,hi=0;
   if(l1<=11){cls=CLS_SHORT;for(unsigned j=0;j<l1&&j<8;++j)k0|=(uint64_t)(unsigned char)s1[j]<<(8*j);for(unsigned j=8;j<l1;++j)k1|=(uint64_t)(unsigned char)s1[j]<<(8*(j-8));k1|=(uint64_t)(l1+1)<<24;}
   else if(s1[0]=='u'&&l1==33){bool g=true;const uint64_t h=hex16_ref(s1+1,g),l=hex16_ref(s1+17,g);if(g){cls=CLS_LONG;lo=l;hi=h;}}
   o.ucls[i]=cls;o.uk0[i]=k0;o.uk1[i]=k1;o.ulo[i]=lo;o.uhi[i]=hi;}
 }
 o.ok=(uint8_t)okm;o.pur=(uint8_t)pum;
}
// ---------------- the scan: three windows per row, and where the next line starts ----------------
// s: the comma and quote masks of the three windows and the window starts bB (one past the first comma of A; p + 65
// if A has none) and bC (one past the second comma of B; bB + 65 if B has fewer than two). nx[i] = one past the first
// '\n' of window C. Result: bit i set = nx[i] is NOT certain to be the line's end (a '\n' at or before the comma that
// ends window A's or window B's part, one of those commas missing, or no '\n' in window C): the caller finds that
// row's newline itself (memchr from ls[i]). A clear bit: nx[i] - 1 is the first '\n' at or after ls[i].
#define VERTG_ROW(i) {const char* const p=ls[i]; \
  const __m512i x=_mm512_loadu_si512((const void*)p); \
  const uint64_t ca=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc)),na=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vn)); \
  s.cA[i]=ca;s.qA[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq)); \
  const uint64_t e0=_tzcnt_u64(ca);const char* const bb=p+e0+1; \
  const __m512i y=_mm512_loadu_si512((const void*)bb); \
  const uint64_t cb=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vc)),nb=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vn)),cx=_blsr_u64(cb); \
  s.cB[i]=cb;s.qB[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vq));s.bB[i]=(uint64_t)bb; \
  const uint64_t e2=_tzcnt_u64(cx);const char* const bc=bb+e2+1; \
  const __m512i z=_mm512_loadu_si512((const void*)bc); \
  s.cC[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(z,vc));s.qC[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(z,vq));s.bC[i]=(uint64_t)bc; \
  const uint64_t nc=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(z,vn)),e3=_tzcnt_u64(nc); \
  nx[i]=bc+e3+1; \
  const uint64_t bad_=(na&(ca^(ca-1)))|(nb&(cx^(cx-1)))|((e0|e2|e3)>>6); /* (ca == 0: ca ^ (ca - 1) is all ones, and e0 is 64 anyway) */ \
  unc|=(unsigned)(bad_!=0)<<i;__asm__("":"+r"(unc));}
VERT_T5 static inline unsigned scang8_v5(const char* const* ls,const char** nx,Ev8gScan& s){
 const __m512i vc=_mm512_set1_epi8(','),vq=_mm512_set1_epi8('"'),vn=_mm512_set1_epi8('\n');
 unsigned unc=0;
 VERTG_ROW(0) VERTG_ROW(1) VERTG_ROW(2) VERTG_ROW(3) VERTG_ROW(4) VERTG_ROW(5) VERTG_ROW(6) VERTG_ROW(7)
 return unc;
}
#undef VERTG_ROW
// ---------------- vector pieces for the IDs ----------------
// Eight ASCII hex digits per lane (first digit in the lowest byte) -> their value; bad collects a non-zero byte for
// every character that is not 0-9 a-f.
VERT_T5 static inline __attribute__((always_inline)) __m512i hex8k_v5(__m512i w,__m512i& bad){
 const __m512i d=_mm512_sub_epi8(w,_mm512_set1_epi8('0')),a=_mm512_sub_epi8(w,_mm512_set1_epi8('a'));
 bad=_mm512_or_si512(bad,_mm512_min_epu8(_mm512_subs_epu8(d,_mm512_set1_epi8(9)),_mm512_subs_epu8(a,_mm512_set1_epi8(5))));
 const __m512i nib=_mm512_mask_add_epi8(d,_mm512_cmpgt_epi8_mask(w,_mm512_set1_epi8(0x40)),a,_mm512_set1_epi8(10));
 const __m512i by=_mm512_maddubs_epi16(nib,_mm512_set1_epi16(0x0110));          // two digits -> one byte value per 16-bit word
 const __m512i dw=_mm512_madd_epi16(by,_mm512_set1_epi32(0x00010100));           // two of those -> 16 bits per 32-bit word
 return _mm512_or_si512(_mm512_slli_epi64(_mm512_and_si512(dw,_mm512_set1_epi64(0xFFFF)),16),_mm512_srli_epi64(dw,32));
}
// 32 hex digits in W0..W3 (text bytes 1..8, 9..16, 17..24, 25..32) -> hi / lo; good = every character is a hex digit.
VERT_T5 static inline __attribute__((always_inline)) __mmask8 hex32k_v5(__m512i W0,__m512i W1,__m512i W2,__m512i W3,__m512i& hi,__m512i& lo){
 __m512i bad=_mm512_setzero_si512();
 const __m512i v0=hex8k_v5(W0,bad),v1=hex8k_v5(W1,bad),v2=hex8k_v5(W2,bad),v3=hex8k_v5(W3,bad);
 hi=_mm512_or_si512(_mm512_slli_epi64(v0,32),v1);lo=_mm512_or_si512(_mm512_slli_epi64(v2,32),v3);
 return _mm512_testn_epi64_mask(bad,bad);
}
// ---------------- the decode ----------------
VERT_T5 static inline void decg8_v5(const char* const* ls,const char* const* nx,const Ev8gScan& s,const EvConst& c,Ev8gOut& o){
 using namespace vert4;
 #define VG_C(x) _mm512_set1_epi64((long long)(x))
 #define VG_TZ(x,xm) _mm512_sub_epi64(c63,_mm512_lzcnt_epi64(_mm512_xor_si512(x,xm))) /* tzcnt(x) given xm = x-1 (63 for x == 0) */
 #define VG_ERR(q,st,en,sq,op) const __m512i sq=_mm512_and_si512(q,st),op=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq,en),ones),en)
 const __m512i one=VG_C(1),ones=VG_C(-1),c63=VG_C(63),c64=VG_C(64);
 const __m512i P=_mm512_loadu_si512((const void*)ls),NL=_mm512_add_epi64(_mm512_loadu_si512((const void*)nx),ones);
 const __m512i BB=_mm512_load_si512((const void*)s.bB),Bv=_mm512_load_si512((const void*)s.bC);
 const __m512i CA=_mm512_load_si512((const void*)s.cA),QA=_mm512_load_si512((const void*)s.qA),CB=_mm512_load_si512((const void*)s.cB),QB=_mm512_load_si512((const void*)s.qB);
 const __m512i C2=_mm512_load_si512((const void*)s.cC),Q2=_mm512_load_si512((const void*)s.qC);
 // window A: one field
 const __m512i tA=_mm512_add_epi64(CA,ones),c0=_mm512_sub_epi64(_mm512_sub_epi64(BB,P),one),mA=_mm512_xor_si512(CA,tA),enA=_mm512_andnot_si512(_mm512_and_si512(CA,tA),CA);
 const __m512i qa=_mm512_and_si512(QA,mA);
 VG_ERR(qa,one,enA,sqA,opA);
 const __m512i errA=_mm512_xor_si512(opA,_mm512_slli_epi64(_mm512_andnot_si512(one,qa),1));
 // window B: two fields
 const __m512i t0=_mm512_add_epi64(CB,ones),e1=VG_TZ(CB,t0),cX=_mm512_and_si512(CB,t0);
 const __m512i t1=_mm512_add_epi64(cX,ones),mB=_mm512_xor_si512(cX,t1),cY=_mm512_and_si512(cX,t1);
 const __m512i e2=_mm512_sub_epi64(_mm512_sub_epi64(Bv,BB),one);                 // tzcnt(cX), 64 if there is no second comma
 const __m512i q1=_mm512_and_si512(QB,mB),st1=_mm512_or_si512(_mm512_slli_epi64(_mm512_xor_si512(CB,cX),1),one),en1=_mm512_xor_si512(CB,cY);
 VG_ERR(q1,st1,en1,sq1,op1);
 const __m512i err1=_mm512_xor_si512(op1,_mm512_slli_epi64(_mm512_andnot_si512(st1,q1),1));
 // window C: version 4's second window
 const __m512i L=_mm512_sub_epi64(NL,Bv),nbit=_mm512_sllv_epi64(one,_mm512_and_si512(L,c63)),keep=_mm512_add_epi64(nbit,ones);
 const __m512i d=_mm512_and_si512(C2,keep),qb=_mm512_and_si512(Q2,keep);
 const __m512i st2=_mm512_or_si512(_mm512_slli_epi64(d,1),one),en2=_mm512_or_si512(d,nbit);
 VG_ERR(qb,st2,en2,sq2,op2);
 const __m512i err2=_mm512_xor_si512(op2,_mm512_slli_epi64(_mm512_andnot_si512(st2,qb),1));
 const __m512i u0=_mm512_add_epi64(d,ones),d0=VG_TZ(d,u0),dA=_mm512_and_si512(d,u0);
 const __m512i u1=_mm512_add_epi64(dA,ones),d1=VG_TZ(dA,u1),dB=_mm512_and_si512(dA,u1);
 const __m512i u2=_mm512_add_epi64(dB,ones),d2=VG_TZ(dB,u2),dR=_mm512_and_si512(dB,u2);
 const __m512i err=_mm512_or_si512(_mm512_ternarylogic_epi64(err1,err2,dR,0xFE),errA);  // a quote error, or a fourth comma before the newline
 __mmask8 ok=_mm512_testn_epi64_mask(err,err);
 ok=_mm512_mask_test_epi64_mask(ok,dB,dB);                                      // three commas in window C
 ok=_mm512_mask_test_epi64_mask(ok,CA,CA);                                      // a comma in window A
 ok=_mm512_mask_cmplt_epu64_mask(ok,_mm512_or_si512(e2,L),c64);                 // second comma of B and the newline inside their windows
 const __m512i z0=_mm512_and_si512(QA,one),l0=_mm512_sub_epi64(c0,_mm512_add_epi64(z0,z0));
 ok=_mm512_mask_test_epi64_mask(ok,l0,l0);                                      // event_id not empty
 const __m512i z1=_mm512_and_si512(QB,one),l1=_mm512_sub_epi64(e1,_mm512_add_epi64(z1,z1));
 const __m512i a2=_mm512_add_epi64(e1,one),z2=_mm512_and_si512(_mm512_srlv_epi64(q1,a2),one),o2=_mm512_add_epi64(a2,z2),l2=_mm512_sub_epi64(_mm512_sub_epi64(e2,o2),z2);
 const __m512i z3=_mm512_and_si512(qb,one),l3=_mm512_sub_epi64(d0,_mm512_add_epi64(z3,z3));
 const __m512i a4=_mm512_add_epi64(d0,one),z4=_mm512_and_si512(_mm512_srlv_epi64(qb,a4),one),o4=_mm512_add_epi64(a4,z4),l4=_mm512_sub_epi64(_mm512_sub_epi64(d1,o4),z4);
 const __m512i a5=_mm512_add_epi64(d1,one),z5=_mm512_and_si512(_mm512_srlv_epi64(qb,a5),one),o5=_mm512_add_epi64(a5,z5),l5=_mm512_sub_epi64(_mm512_sub_epi64(d2,o5),z5);
 const __m512i a6=_mm512_add_epi64(d2,one),z6=_mm512_and_si512(_mm512_srlv_epi64(qb,a6),one),o6=_mm512_add_epi64(a6,z6),l6=_mm512_sub_epi64(_mm512_sub_epi64(L,o6),z6);
 // ---- operands: the field addresses go through memory into general registers, 16-byte loads, no gather ----
 // Every address is at most line start + 200 (kind and price are limited to that: an ok row has them below line start
 // + 192), so every load stays inside the 224 readable bytes.
 const __m512i S0=_mm512_add_epi64(P,z0),S1=_mm512_add_epi64(BB,z1),S2=_mm512_add_epi64(BB,o2),S3=_mm512_add_epi64(Bv,z3),S6=_mm512_add_epi64(Bv,o6);
 const __m512i top=_mm512_add_epi64(P,VG_C(200));
 alignas(64) uint64_t AD[48];
 _mm512_store_si512((void*)AD,S0);_mm512_store_si512((void*)(AD+8),S1);_mm512_store_si512((void*)(AD+16),S2);_mm512_store_si512((void*)(AD+24),S3);
 _mm512_store_si512((void*)(AD+32),_mm512_min_epu64(_mm512_add_epi64(Bv,o4),top));_mm512_store_si512((void*)(AD+40),_mm512_min_epu64(S6,top));
 const uint64_t* ad=AD;__asm__("":"+r"(ad)::"memory");
 __m512i E0,E1,E2,E3,E4,Ex,U0,U1,U2,U3,U4,Ux,ts0,tsh,ts2,tsx,ig0,ig1,igl,ig2,KW,kh,PW,ph;
 ld16x8_v5(ad,0,E0,E1);          // event_id bytes 0..15
 ld16x8_v5(ad,9,E2,E3);          // 9..24
 ld16x8_v5(ad,25,E4,Ex);         // 25..40
 ld16x8_v5(ad+8,0,U0,U1);        // user_id bytes 0..15
 ld16x8_v5(ad+8,9,U2,U3);
 ld16x8_v5(ad+8,25,U4,Ux);
 ld16x8_v5(ad+16,0,ts0,tsh);     // ts bytes 0..15
 ld16x8_v5(ad+16,12,ts2,tsx);    // ts bytes 12..27
 ld16x8_v5(ad+24,0,ig0,ig1);     // ingest_ts bytes 0..15
 ld16x8_v5(ad+24,4,igl,ig2);     // ingest_ts bytes 4..19 (the upper half is bytes 12..19)
 ld16x8_v5(ad+32,0,KW,kh);       // event_type
 ld16x8_v5(ad+40,0,PW,ph);       // price_cents
 (void)igl;(void)kh;(void)ph;(void)Ex;(void)Ux;
 const __m512i lowbyte=VG_C(0xFF),c8=VG_C(8),c33=VG_C(33);
 // ---- event_id: class, key ----
 __m512i ecls,ekey,ehi;
 {const __m512i W=_mm512_or_si512(_mm512_srli_epi64(E0,8),_mm512_slli_epi64(E1,56));       // text bytes 1..8
  const __mmask8 let=_mm512_cmpeq_epi64_mask(_mm512_and_si512(E0,lowbyte),VG_C('e'));
  const __m512i n=_mm512_sub_epi64(l0,one);                                                 // bytes after the letter (l0 >= 1 in an ok row)
  const __m512i keepb=_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(n,3))); // n == 0 or n > 8: the shift count is 64 or more and gives 0 (n > 8 is not short anyway)
  const __m512i w=_mm512_and_si512(W,keepb);
  const __m512i x=_mm512_or_si512(w,_mm512_andnot_si512(keepb,_mm512_set1_epi8('0')));
  const __m512i dd=_mm512_sub_epi8(x,_mm512_set1_epi8('0')),aa=_mm512_sub_epi8(x,_mm512_set1_epi8('a'));
  const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(dd,_mm512_set1_epi8(9)),_mm512_subs_epu8(aa,_mm512_set1_epi8(25)));
  const __mmask8 sh=_mm512_mask_testn_epi64_mask(_mm512_mask_cmple_epu64_mask(let,n,c8),bad,bad);
  __m512i hi,lo;const __mmask8 lg=_kand_mask8(_mm512_mask_cmpeq_epi64_mask(let,l0,c33),hex32k_v5(W,E2,E3,E4,hi,lo));
  ecls=_mm512_mask_mov_epi64(_mm512_mask_mov_epi64(VG_C(CLS_OTHER),lg,one),sh,_mm512_setzero_si512());
  ekey=_mm512_mask_mov_epi64(_mm512_mask_mov_epi64(S0,lg,lo),sh,w);
  ehi=_mm512_maskz_mov_epi64(lg,hi);}
 // ---- user_id: class, KeyTable words, long value ----
 __m512i ucls,uk0,uk1,ulo,uhi;
 {const __m512i W=_mm512_or_si512(_mm512_srli_epi64(U0,8),_mm512_slli_epi64(U1,56));
  const __mmask8 let=_mm512_cmpeq_epi64_mask(_mm512_and_si512(U0,lowbyte),VG_C('u'));
  const __mmask8 pk=_mm512_cmple_epu64_mask(l1,VG_C(11));
  const __m512i n0=_mm512_min_epu64(l1,c8),n1=_mm512_sub_epi64(l1,n0);                      // text bytes in k0 / in k1
  const __m512i k0=_mm512_and_si512(U0,_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(n0,3))));
  const __m512i k1=_mm512_or_si512(_mm512_and_si512(U1,_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(n1,3)))),_mm512_slli_epi64(_mm512_add_epi64(l1,one),24));
  __m512i hi,lo;const __mmask8 lg=_kand_mask8(_mm512_mask_cmpeq_epi64_mask(let,l1,c33),hex32k_v5(W,U2,U3,U4,hi,lo));
  ucls=_mm512_mask_mov_epi64(_mm512_mask_mov_epi64(VG_C(CLS_OTHER),lg,one),pk,_mm512_setzero_si512());
  uk0=_mm512_maskz_mov_epi64(pk,k0);uk1=_mm512_maskz_mov_epi64(pk,k1);ulo=_mm512_maskz_mov_epi64(lg,lo);uhi=_mm512_maskz_mov_epi64(lg,hi);}
 const __mmask8 hasu=_mm512_test_epi64_mask(l1,l1);
 // ---- ingest_ts, ts, kind, validity, price: the text of version 4 ----
 const __m512i cZ=VG_C('Z'),c20=VG_C(20);
 __mmask8 oki,hiti,oks,hits,okoff,okep,knz;
 const __m512i todi=tod8k_v5(ig1,ig2,oki);
 const __m512i dayi=day4k_v5(ig0,ig1,c.d,hiti);
 const __m512i ingest=_mm512_add_epi64(dayi,todi);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_mask_cmpeq_epi64_mask(_kand_mask8(ok,_kand_mask8(oki,hiti)),_mm512_srli_epi64(ig2,56),cZ),l3,c20);
 const __mmask8 is20=_mm512_cmpeq_epi64_mask(l2,c20),is25=_mm512_cmpeq_epi64_mask(l2,VG_C(25)),is13=_mm512_cmpeq_epi64_mask(l2,VG_C(13));
 const __m512i ts1=_mm512_mask_or_epi64(tsh,is13,_mm512_srli_epi64(ts0,40),_mm512_slli_epi64(tsh,24)); // bytes 8..15, for 13 digits bytes 5..12
 const __m512i ts3=_mm512_or_si512(_mm512_srli_epi64(ts2,40),_mm512_slli_epi64(tsx,24));               // bytes 17..24
 const __m512i tods=tod8k_v5(ts1,ts2,oks);
 const __m512i days=day4k_v5(ts0,ts1,c.d,hits);
 const __m512i off=off8k_v5(ts3,okoff);
 const __m512i ep=epoch8k_v5(ts0,ts1,okep);
 const __m512i b19=_mm512_srli_epi64(ts2,56);
 const __mmask8 z20=_mm512_mask_cmpeq_epi64_mask(is20,b19,cZ),p25=_mm512_mask_cmpeq_epi64_mask(is25,b19,VG_C('+')),m25=_mm512_mask_cmpeq_epi64_mask(is25,b19,VG_C('-'));
 __m512i ts=_mm512_add_epi64(days,tods);
 ts=_mm512_mask_sub_epi64(ts,p25,ts,off);
 ts=_mm512_mask_add_epi64(ts,m25,ts,off);
 ts=_mm512_mask_mov_epi64(ts,is13,ep);
 ok=_kand_mask8(ok,_kor_mask8(_kand_mask8(is13,okep),_kand_mask8(_kand_mask8(oks,hits),_kor_mask8(z20,_kand_mask8(okoff,_kor_mask8(p25,m25))))));
 const __m512i kind=kind8k_v5(KW,l4,knz);
 const __m512i tsrel=_mm512_sub_epi64(ts,VG_C(c.wstart));
 __mmask8 valid=_mm512_mask_cmplt_epu64_mask(_mm512_mask_test_epi64_mask(_kand_mask8(knz,hasu),l5,l5),tsrel,VG_C(c.wlen));
 valid=_mm512_mask_cmple_epi64_mask(valid,_mm512_sub_epi64(ingest,ts),VG_C(DAYMS));
 __m512i k=_mm512_slli_epi64(_mm512_add_epi64(tsrel,one),EV_TS_SHIFT);
 const __mmask8 pv=_mm512_mask_cmpeq_epi64_mask(valid,kind,VG_C(3));
 __mmask8 pur;
 {const __m512i n8=_mm512_slli_epi64(l6,3);
  const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(PW,_mm512_sub_epi64(c64,n8)),_mm512_srlv_epi64(_mm512_set1_epi8('0'),n8));
  const __m512i v=_mm512_sub_epi8(x,_mm512_set1_epi8('0')),nine=_mm512_set1_epi8(9);
  const __m512i p4=_mm512_madd_epi16(_mm512_maddubs_epi16(v,_mm512_set1_epi16(0x010A)),_mm512_set1_epi32(0x00010064));
  const __m512i price=_mm512_add_epi64(_mm512_mul_epu32(p4,VG_C(10000)),_mm512_srli_epi64(p4,32));
  pur=_mm512_mask_cmple_epu64_mask(_mm512_mask_cmpeq_epi64_mask(pv,_mm512_max_epu8(v,nine),nine),_mm512_sub_epi64(price,one),VG_C(PRICE_MAX-1)); // digits, 1..10000000 (an empty field gives 0)
  ok=_kandn_mask8(_mm512_mask_cmplt_epu64_mask(pv,c8,l6),ok);                    // a longer price field: the line code decides
  k=_mm512_mask_or_epi64(k,pur,k,_mm512_or_si512(price,VG_C(EV_PURCHASE_BIT)));
 }
 valid=_kand_mask8(valid,_kor_mask8(_knot_mask8(pv),pur));                      // a purchase with a bad price is not a valid event
 _mm512_store_si512((void*)o.ecls,ecls);_mm512_store_si512((void*)o.ekey,ekey);_mm512_store_si512((void*)o.ehi,ehi);
 _mm512_store_si512((void*)o.es,S0);_mm512_store_si512((void*)o.el,l0);
 _mm512_store_si512((void*)o.ucls,ucls);_mm512_store_si512((void*)o.uk0,uk0);_mm512_store_si512((void*)o.uk1,uk1);
 _mm512_store_si512((void*)o.ulo,ulo);_mm512_store_si512((void*)o.uhi,uhi);
 _mm512_store_si512((void*)o.us,S1);_mm512_store_si512((void*)o.ul,l1);
 _mm512_store_si512((void*)o.kk,_mm512_maskz_mov_epi64(valid,k));
 _mm512_store_si512((void*)o.ingest,ingest);
 _mm512_store_si512((void*)o.s5,_mm512_add_epi64(Bv,o5));_mm512_store_si512((void*)o.l5,l5);
 _mm512_store_si512((void*)o.s6,S6);_mm512_store_si512((void*)o.l6,l6);
 o.ok=(uint8_t)_cvtmask8_u32(ok);o.pur=(uint8_t)_cvtmask8_u32(_kand_mask8(pur,ok));
 #undef VG_C
 #undef VG_TZ
 #undef VG_ERR
}
// One call for a group whose line ends are known (tests; the lane loop calls scang8_v5 one group ahead).
VERT_T5 static inline void ev8g_v5(const char* const* ls,const char* const* nx,const EvConst& c,Ev8gOut& o){
 Ev8gScan s;const char* nn[8];(void)scang8_v5(ls,nn,s);decg8_v5(ls,nx,s,c,o);
}
} // namespace vertg
// ==== end of ev8g.h ====
// ---- lanes, events: the scan stage of ev8 version 4 that also yields where the next line of each row starts, so
// ---- that eight "lanes" can walk eight pieces of a chunk without a line index. /home/claude/vert/lane_ev_paste.h;
// ---- tested as this exact text by lanes/lane_test.cpp (Ev8Scan == vert4::scan8t_v5<1> byte for byte; nx == memchr).
// ---- Paste after ev8_paste4.h (it uses vert4::Ev8Scan and VERT_T5). The decode is the unchanged vert4::dec8_v5.
namespace vlane{
// s: exactly what vert4::scan8t_v5<1>(ls, s) stores. Result 0: every nx[i] is one past the first '\n' at or after
// ls[i] (the rule of the line index: every '\n' ends a line, quoted or not). Result not 0 (0xFF): no nx[i] may be
// used and the caller finds the eight newlines itself (memchr from ls[i]). The result is 0 only if, for all eight
// rows, window 1 (ls[i] .. +63) holds no '\n', its third comma exists (so window 2 starts at b <= ls[i]+64 and
// [ls[i], b) lies inside window 1) and window 2 (b .. b+63) holds a '\n': then the first '\n' of window 2 is the first
// one of the line. Reads the bytes scan8t_v5 reads (64 at ls[i], 64 at b <= ls[i]+65), nothing else.
// e2 = tzcnt of "commas after the second" (64: no third comma), e3 = tzcnt of window 2's newlines (64: none): bit 6
// of the OR of all of them says that some row lacks one of the two; kx collects the newlines of the first windows.
#define VLANE_EV_ROW(i) {const char* const p=ls[i]; \
  const __m512i x=_mm512_loadu_si512((const void*)p); \
  const uint64_t cm=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc)); \
  s.c1[i]=cm;s.q1[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq)); \
  _mm_storeu_si128((__m128i*)&s.h[2*(i)],_mm512_castsi512_si128(x)); \
  kx=_kor_mask64(kx,_mm512_cmpeq_epi8_mask(x,vn)); \
  const uint64_t e2=_tzcnt_u64(_blsr_u64(_blsr_u64(cm))); \
  const char* const b=p+e2+1; /* one past the third comma (p+65 if there is none) */ \
  const __m512i y=_mm512_loadu_si512((const void*)b); \
  s.c2[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vc));s.q2[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vq));s.b[i]=(uint64_t)b; \
  const uint64_t e3=_tzcnt_u64(_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vn))); \
  nx[i]=b+e3+1;acc|=e2|e3;__asm__("":"+r"(acc));} /* (the empty asm keeps the OR here: otherwise the compiler holds all sixteen counts in registers to the end) */
VERT_T5 static inline unsigned lscan8_v5(const char* const* ls,const char** nx,vert4::Ev8Scan& s){
 const __m512i vc=_mm512_set1_epi8(','),vq=_mm512_set1_epi8('"'),vn=_mm512_set1_epi8('\n');
 __mmask64 kx=_cvtu64_mask64(0);uint64_t acc=0;
 VLANE_EV_ROW(0) VLANE_EV_ROW(1) VLANE_EV_ROW(2) VLANE_EV_ROW(3) VLANE_EV_ROW(4) VLANE_EV_ROW(5) VLANE_EV_ROW(6) VLANE_EV_ROW(7)
 return ((acc>>6)|(uint64_t)(_cvtmask64_u64(kx)!=0))!=0?0xFFu:0u;
}
#undef VLANE_EV_ROW
} // namespace vlane
// The scalar kernel in the two-step shape of the vector one (nothing to scan), as a call of its own: the avx2
// loop, a test stand-in only (also what callgrind and the sanitizer runs see).
static inline void ev8_scan_none(const char* const*,vert::Ev8Scan&){}
[[gnu::noinline]] static void ev8_dec_ref(const char* const* ls,const char* const* nx,const vert::Ev8Scan&,const vert::EvConst& c,vert::Ev8Out& o){vert::ev8_ref(ls,nx,c,o);}
static inline void ev8_scan_none4(const char* const*,vert4::Ev8Scan&){}
[[gnu::noinline]] static void ev8_dec_ref4(const char* const* ls,const char* const* nx,const vert4::Ev8Scan&,const vert4::EvConst& c,vert4::Ev8Out& o){vert4::ev8_ref(ls,nx,c,o);}
// The constants of a loop call for either kernel version (EvLoop is defined above). Version 4 gets the months of
// the run's own window: without `d` it would accept no date and leave every row to the line code.
#define SS_EV8_CONST3(lp_) vert::EvConst{lp_.ke,lp_.limE,lp_.wstart,lp_.wlen,(const vert::DateEnt*)lp_.memo,lp_.tid48}
static inline vert4::EvConst ev8_const4(uint64_t ke,uint64_t limE,int64_t wstart,uint64_t wlen,uint64_t tid48){
 vert4::EvConst ec{ke,limE,wstart,wlen,nullptr,tid48};ec.d=vert4::make_dates(window_start,window_end);return ec;}
#define SS_EV8_CONST4(lp_) ev8_const4(lp_.ke,lp_.limE,lp_.wstart,lp_.wlen,lp_.tid48)
// Finishing the first N rows of a group in line order. Purchases first get their category (reads only).
// A group whose eight rows are all ok takes the short way: no row test, and the highest counter of the group
// goes into vmx (the low halves of four 64-bit lanes; fastAny says that vmx holds anything).
#define SS_EV8_SLOT(O_,i_) (*(ESlot*)((char*)lp.ev+O_.so[i_]))
#define SS_EV8_COMMIT(O_,i_) eslot_commit(SS_EV8_SLOT(O_,i_),O_.kk[i_],O_.uct[i_],(int64_t)O_.ingest[i_],O_.uw[i_]);
// Stream mode (lp.strm): the row goes behind the stream of its bucket instead (the kernel's uct has no thread then).
#define SS_EV8_APPEND(O_,i_) estream_put(*lp.es,lp.bsh,O_.ve[i_],O_.kk[i_],O_.uct[i_],(int64_t)O_.ingest[i_],O_.uw[i_]);
#define SS_EV8_COMMIT1(O_,i_) {if(lp.strm){SS_EV8_APPEND(O_,i_)}else{SS_EV8_COMMIT(O_,i_)}}
#define SS_EV8_COMMIT8(O_) {if(!lp.strm){SS_EV8_COMMIT(O_,0) SS_EV8_COMMIT(O_,1) SS_EV8_COMMIT(O_,2) SS_EV8_COMMIT(O_,3) SS_EV8_COMMIT(O_,4) SS_EV8_COMMIT(O_,5) SS_EV8_COMMIT(O_,6) SS_EV8_COMMIT(O_,7)} \
  else if(lp.strm8)estream_put8_v5(*lp.es,lp.bsh,O_);else estream_put8_rows(*lp.es,lp.bsh,O_);}
// Stream mode, at the time the direct commit asks for its slots (a group ahead of the append): with thousands of
// buckets the fill lines and states of a thread no longer fit its share of the second-level cache, so the eight
// rows' lines are asked for here (lp.spf: SS_SPF, default on from 1,000 buckets).
#define SS_EV8_SPF(O_,M) {for(unsigned x_=(M);x_;x_&=x_-1){const size_t b_=(size_t)(O_.ve[__builtin_ctz(x_)]>>lp.bsh);__builtin_prefetch(lp.es->fill+b_*StreamStore<ERec>::RPL,1,3);__builtin_prefetch(lp.es->bs+b_,1,3);}}
#define SS_EV8_FINISH(O,LS,NX,N,SLOW) {auto& O_=(O);const char* const* const L_=(LS);const char* const* const X_=(NX);const unsigned n_=(N); \
  const unsigned ok_=O_.ok&((1u<<n_)-1);rows+=n_; \
  for(unsigned m_=O_.pur&ok_;m_;m_&=m_-1){const unsigned i_=(unsigned)__builtin_ctz(m_); \
   const uint32_t ci_=products.find(string_view(O_.s5[i_],(size_t)O_.l5[i_]));O_.uct[i_]|=(uint64_t)(ci_==KeyTable::NONE?0u:(unsigned)(uint16_t)ci_)<<32;} \
  if(__builtin_expect(ok_==0xFFu,1)){ \
   vmx=_mm256_max_epu32(vmx,_mm256_max_epu32(_mm256_load_si256((const __m256i*)&O_.ve[0]),_mm256_load_si256((const __m256i*)&O_.ve[4])));fastAny=true; \
   SS_EV8_COMMIT8(O_) \
  }else for(unsigned i_=0;i_<n_;++i_){ \
   if((ok_>>i_)&1){send=max(send,(uint32_t)O_.ve[i_]+1);SS_EV8_COMMIT1(O_,i_)} \
   else SLOW(L_[i_],X_[i_]-1,((O_.idok>>i_)&1)?(uint32_t)O_.ve[i_]:NOPRE,lp,dmax,send,general);} }
// The line index of this loop. ls[pos..m]: the starts of the lines not yet in a group; ls[pos..m-1] have their
// newline (ls[i+1]-1), the line at ls[m] not yet (the index has not got that far, or the file ends without
// one). Three groups are under way at any time, each one step further than the next:
//   N  just formed from the index: SCAN (the window masks of its lines, kept in one of two Ev8Scan)
//   D  formed a turn ago: DEC (everything else, from its own scan) and the requests for its slots
//   F  decoded a turn ago: finished (SS_EV8_FINISH), so every slot has had eight lines and a kernel call of time
// The masks a DEC loads were stored a whole turn earlier (a load right behind its eight stores would wait for
// them). The groups that are under way keep their eight starts each right before ls[pos]; a group's scan is
// always the scan of exactly its lines.
static constexpr size_t EV8_BLOCK=4096; // bytes indexed at a time
#define SS_EV8_PF(O_,i_) __builtin_prefetch(&SS_EV8_SLOT(O_,i_),1,3);
// SS_PF picks the instruction of the slot requests of the ev8 and cl8 loops, for measuring on the scoring machine:
// default PREFETCHT0 (what __builtin_prefetch(p,1,3) gives without the prfchw target); w = PREFETCHW (line asked
// for in exclusive state; opt-in, no CPU test: every x86-64 CPU with AVX-512 has it); t1 = PREFETCHT1 (into the
// second-level cache only); none = no request at all. Same output for all.
static unsigned optPfw=0; // 0 default, 1 w, 2 t1, 3 none
static unsigned optClickWin2=1; // click lines of 64+ bytes: 1 = three windows in the line code (click_long_fields), 0 = the general handler (SS_CLWIN=1), 2 = three windows, every such line checked against the portable splitter (SS_CLWIN=check)
#define SS_PFW_ASM(p_) {if(pfw_==1)__asm__ volatile("prefetchw %0"::"m"(*(const char*)(p_)));else if(pfw_==2)__asm__ volatile("prefetcht1 %0"::"m"(*(const char*)(p_)));}
#define SS_EV8_PFW(O_,i_) SS_PFW_ASM(&SS_EV8_SLOT(O_,i_))
// NAME_slow: a row the kernel did not decide goes through the line code, kept out of the loop so that the loop
// stays small (`have` = the kernel's counter, or NOPRE = no claim: then the line's ID is decoded here).
#define SS_EVENT_LOOP_EV8(NAME,TARGET,MASKS,NLMASK,SCAN,DEC,VNS,ECONST) \
template<class GEN> TARGET [[gnu::noinline]] static void NAME##_slow(const char* const q,const char* const nl,const uint32_t have,const EvLoop& lp,uint32_t& dmax,uint32_t& send,GEN&& general){ \
 PendUser pend[1]={};PendUser* pp=pend;unsigned rpos=0; /* no pending batch, no ring in the dense-record line code */ \
 ++lp.tl.ev8Slow; \
 const uint32_t pre=have!=NOPRE?have:event_pre_v3(q,lp); \
 const WinMasks w1=MASKS(q);const char* const bb=event_window2(q,w1.c); \
 event_line_v3<true,true>(q,bb,w1,MASKS(bb),nl,pre,lp,pend,pp,dmax,send,(ERing*)nullptr,rpos,general); \
} \
template<unsigned LEAD,class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const char* const end=base+len; \
 const EvLoop lp(tl,tab,products,PE,end);uint32_t dmax=0;uint32_t send=0; \
 const char* ls[EV8_BLOCK+64+64];size_t pos=0,m=0;ls[0]=p;const char* scan=p; \
 const VNS::EvConst ec=ECONST(lp);const unsigned pfw_=optPfw; \
 VNS::Ev8Out out[3];VNS::Ev8Scan sc[2];const char* tls[8];const char* tnx[8]; \
 const char* const* lN=ls;const char* const* xN=ls;const char* const* lD=ls;const char* const* xD=ls;const char* const* lW=ls;const char* const* xW=ls;const char* const* lF=ls;const char* const* xF=ls; /* lines and next lines of N, D, W, F */ \
 unsigned nD=0,nW=0,nF=0;VNS::Ev8Scan* sN=&sc[0];VNS::Ev8Scan* sD=&sc[1];VNS::Ev8Out* oD=&out[0];VNS::Ev8Out* oW=&out[2];VNS::Ev8Out* oF=&out[1];bool more=true; /* rows of D, W and F; N's and D's scan; D's, W's and F's results. W (LEAD 2 only): a decoded group that waits one more turn */ \
 __m256i vmx=_mm256_setzero_si256();bool fastAny=false;uint64_t rows=0; \
 while(more||nD||nW||nF){ \
  unsigned n=0; \
  if(more){ \
   if(pos+8>m){ /* fewer than eight whole lines known: what is still needed moves to the front and the index goes on */ \
    const size_t s_=pos-(nD?8:0)-(nW?8:0)-(nF?8:0); \
    if(s_){for(size_t i_=0;i_<=m-s_;i_+=4)_mm256_storeu_si256((__m256i*)(ls+i_),_mm256_loadu_si256((const __m256i*)(ls+s_+i_)));pos-=s_;m-=s_; \
     if(nD){lD-=s_;xD-=s_;} if(nW){lW-=s_;xW-=s_;} if(nF){lF-=s_;xF-=s_;}} \
    while(m-pos<8&&scan<end&&ls[m]<lim){ \
     const char* const stop=(size_t)(end-scan)>EV8_BLOCK?scan+EV8_BLOCK:end; \
     const char** w_=ls+m; /* the first newline of a window is stored without a branch (no newline: a value past the last entry, never read); a second one is rare */ \
     do{uint64_t x_=NLMASK(scan);w_[1]=scan+_tzcnt_u64(x_)+1;w_+=(x_!=0);x_=_blsr_u64(x_); \
      while(__builtin_expect(x_!=0,0)){*++w_=scan+_tzcnt_u64(x_)+1;x_=_blsr_u64(x_);} \
      scan+=64;}while(scan<stop); \
     m=(size_t)(w_-ls);} \
   } \
   if(__builtin_expect(pos+8<=m&&ls[pos+7]<lim,1)){n=8;lN=ls+pos;xN=ls+pos+1;} \
   else{ /* the last lines of the chunk: a short group, filled up with copies of its last line (results not used) */ \
    more=false; \
    while(pos+n<m&&ls[pos+n]<lim)++n; \
    if(n){for(unsigned i_=0;i_<8;++i_){const unsigned j_=i_<n?i_:n-1;tls[i_]=ls[pos+j_];tnx[i_]=ls[pos+j_+1];}lN=tls;xN=tnx;} \
   } \
   pos+=n; \
   if(n)SCAN(lN,*sN); \
  } \
  if(nD){ \
   VNS::Ev8Out& o_=*oD; \
   DEC(lD,xD,*sD,ec,o_); \
   if(lp.strm){if(lp.spf)SS_EV8_SPF(o_,o_.idok&((1u<<nD)-1))} /* stream mode: no slot is written here, so none is asked for */ \
   else if(__builtin_expect(pfw_,0)){for(unsigned x_=o_.idok&((1u<<nD)-1);x_;x_&=x_-1)SS_EV8_PFW(o_,__builtin_ctz(x_))} \
   else if(__builtin_expect((nD==8)&(o_.idok==0xFFu),1)){SS_EV8_PF(o_,0) SS_EV8_PF(o_,1) SS_EV8_PF(o_,2) SS_EV8_PF(o_,3) SS_EV8_PF(o_,4) SS_EV8_PF(o_,5) SS_EV8_PF(o_,6) SS_EV8_PF(o_,7)} \
   else for(unsigned x_=o_.idok&((1u<<nD)-1);x_;x_&=x_-1)SS_EV8_PF(o_,__builtin_ctz(x_)) \
  } \
  if(nF)SS_EV8_FINISH(*oF,lF,xF,nF,NAME##_slow) \
  if(LEAD==2){lF=lW;xF=xW;nF=nW;lW=lD;xW=xD;nW=nD;lD=lN;xD=xN;nD=n; /* W becomes F, D becomes W, N becomes D */ \
   VNS::Ev8Out* const u_=oF;oF=oW;oW=oD;oD=u_;} \
  else{lF=lD;xF=xD;nF=nD;lD=lN;xD=xN;nD=n; /* D becomes F, N becomes D */ \
   VNS::Ev8Out* const u_=oD;oD=oF;oF=u_;} \
  {VNS::Ev8Scan* const t_=sN;sN=sD;sD=t_;} \
 } \
 if(pos==m&&ls[m]<lim)line_general<7>(ls[m],end,general); /* the last line has no newline */ \
 if(fastAny){alignas(32) uint32_t v_[8];_mm256_store_si256((__m256i*)v_,vmx);send=max(send,max(max(v_[0],v_[2]),max(v_[4],v_[6]))+1);} \
 tl.denseMax=max(tl.denseMax,dmax);tl.evEnd=max(tl.evEnd,send);tl.ev8Rows+=rows; \
}
// _e8 = kernel version 4 (default), _e8v3 = version 3 (SS_EV8=3, for comparing the two from one binary).
SS_EVENT_LOOP_EV8(events_avx2_e8,SS_V3,win_masks_avx2,nl_mask_avx2,ev8_scan_none4,ev8_dec_ref4,vert4,SS_EV8_CONST4)
SS_EVENT_LOOP_EV8(events_avx512z_e8,SS_V5,win_masks_avx512z,nl_mask_avx512z,vert4::scan8_v5,vert4::dec8_v5,vert4,SS_EV8_CONST4)
SS_EVENT_LOOP_EV8(events_avx512y_e8,SS_V5,win_masks_avx512y,nl_mask_avx512y,vert4::scan8_v5,vert4::dec8_v5,vert4,SS_EV8_CONST4)
SS_EVENT_LOOP_EV8(events_avx2_e8v3,SS_V3,win_masks_avx2,nl_mask_avx2,ev8_scan_none,ev8_dec_ref,vert,SS_EV8_CONST3)
SS_EVENT_LOOP_EV8(events_avx512z_e8v3,SS_V5,win_masks_avx512z,nl_mask_avx512z,vert::scan8_v5,vert::dec8_v5,vert,SS_EV8_CONST3)
SS_EVENT_LOOP_EV8(events_avx512y_e8v3,SS_V5,win_masks_avx512y,nl_mask_avx512y,vert::scan8_v5,vert::dec8_v5,vert,SS_EV8_CONST3)
// ---- lanes: the ev8 loop without a line index (SS_LANES=0 keeps the loop above) ----
// The lines of a chunk are cut into eight pieces at line starts (lane_cut: seven memchr calls) and lane i walks piece
// i: the start of its next line is one past the newline of its current one, and that newline is found by the scan
// stage on the bytes it loads anyway (vlane::lscan8_v5 = the text of vert4::scan8t_v5<1> + two newline compares).
// A line end the scan cannot give for certain (line shorter than 64 bytes, no third comma in the first 64 bytes,
// newline beyond the second window) is searched with memchr from the line start (lane_fix), so the kernel is always
// handed a true (line start, next line start) pair, as from the index: "the first '\n' at or after the start ends the
// line", quoted or not. It then accepts or refuses the row exactly as in the loop above.
// Group g: line starts A[g&3], next starts A[(g+1)&3] = the line starts of group g+1. A lane that has reached the end
// of its piece stays where it is and its rows are masked (the masks nN / nD / nF name the rows that are lines of this
// chunk); the kernel still reads the bytes there (inside the file or its zero padding: at most `end`) but nothing of
// such a row is used. The last line of a file without a newline goes to line_general after the loop, as above.
// Rows are finished in lane order, not file order: nothing in the dense-record loops depends on the order (slots keep
// the smallest ingest_ts, send / dmax are maxima, the line code has no state across lines but the date memo).
// Every full group asks for the cache lines lanePf and lanePf2 bytes ahead of each lane's next line (SS_LANES_PF=n,
// SS_LANES_PF2=n; 0 = that one off). Without them the lanes lose to the index loops: eight streams per thread are not
// covered by the hardware prefetchers, and a miss in a lane's scan load holds up the whole group, where the index
// pass ran through 4 KB at a time with many loads under way (parse-only CPU time of one thread on 3.2M events + 0.8M
// clicks, this box: index loops 178 ms, lanes without prefetch 233, one prefetch 512 or 1024 ahead 151, two 138).
// Two per lane because an event line (77 bytes) is more than one cache line per group.
// Click lines (about 48 bytes) are less than a cache line per group: the click loop asks for the first one only.
// The first lanePf bytes of each lane of a chunk are asked for when the chunk is cut (lane_cut).
static unsigned lanePf=1024,lanePf2=1088;
#define SS_LANE_PF1(X_) if(__builtin_expect(lanePf!=0,1)){const unsigned d_=lanePf;for(unsigned i_=0;i_<8;++i_)__builtin_prefetch((X_)[i_]+d_,0,3);}
#define SS_LANE_PF(X_) if(__builtin_expect(lanePf!=0,1)){const unsigned d_=lanePf;for(unsigned i_=0;i_<8;++i_)__builtin_prefetch((X_)[i_]+d_,0,3); \
  if(lanePf2){const unsigned e_=lanePf2;for(unsigned i_=0;i_<8;++i_)__builtin_prefetch((X_)[i_]+e_,0,3);}}
static inline void lane_cut(const char* p,const char* lim,const char* end,const char** c){
 const size_t step=(size_t)(lim-p)/8;c[0]=p;c[8]=lim;
 for(unsigned i=1;i<8;++i){
  const char* const t=p+step*i; // the first line start at or after t is one past the first newline at or after t-1 (p-1 is a newline)
  const char* const nl=(const char*)memchr(t-1,'\n',(size_t)(end-(t-1)));
  c[i]=nl&&nl+1<lim?nl+1:lim;}
 if(const unsigned d=lanePf)for(unsigned i=0;i<8;++i)if(c[i]<c[i+1])for(unsigned o=0;o<d;o+=64)__builtin_prefetch(c[i]+o,0,3);
}
// Rows whose line end the scan did not give (bits of unc): x[i] = one past the first newline from l[i]. A line
// without one (the last line of the file): returned as a bit, `tail` = the line, the lane goes to the end.
[[gnu::noinline]] static unsigned lane_fix(const char* const* l,const char** x,unsigned unc,const char* end,const char*& tail,uint64_t& looked){
 unsigned gone=0;looked+=(unsigned)__builtin_popcount(unc);
 for(;unc;unc&=unc-1){const unsigned i=(unsigned)__builtin_ctz(unc);
  const char* const nl=(const char*)memchr(l[i],'\n',(size_t)(end-l[i]));
  if(nl)x[i]=nl+1;else{tail=l[i];x[i]=end;gone|=1u<<i;}}
 return gone;
}
// Which lanes still have a line (position below the end of the piece).
static inline unsigned lane_live_scalar(const char* const* l,const char* const* e){unsigned m=0;for(unsigned i=0;i<8;++i)m|=(unsigned)(l[i]<e[i])<<i;return m;}
// (The mask goes through a 16-bit mask move and an explicit AND: g++ 12 otherwise stores the 8-bit mask as one byte
// and reads the slot back as 32 bits.)
SS_V5I unsigned lane_live_avx512(const char* const* l,const char* const* e){
 const __mmask16 k=(__mmask16)_mm512_cmplt_epu64_mask(_mm512_load_si512((const void*)l),_mm512_load_si512((const void*)e));
 return (unsigned)_cvtmask16_u32(k)&0xFFu;}
// The lane scans of the avx2 instances (test stand-ins as the scalar kernels are): the same rule from win_masks_avx2.
SS_V3 [[gnu::noinline]] static unsigned lane_scan_ev_avx2(const char* const* ls,const char** nx,vert4::Ev8Scan&){
 unsigned unc=0;
 for(unsigned i=0;i<8;++i){const char* const p=ls[i];const WinMasks w1=win_masks_avx2(p);const uint64_t t=_blsr_u64(_blsr_u64(w1.c));
  const char* const b=p+_tzcnt_u64(t)+1;const WinMasks w2=win_masks_avx2(b);nx[i]=b+_tzcnt_u64(w2.n)+1;
  if(w1.n!=0||t==0||w2.n==0)unc|=1u<<i;}
 return unc;
}
// Finishing the rows M (a mask) of a group; otherwise SS_EV8_FINISH.
#define SS_EV8L_FINISH(O,LS,NX,M,SLOW) {auto& O_=(O);const char* const* const L_=(LS);const char* const* const X_=(NX);const unsigned m_=(M); \
  const unsigned ok_=O_.ok&m_; \
  for(unsigned p_=O_.pur&ok_;p_;p_&=p_-1){const unsigned i_=(unsigned)__builtin_ctz(p_); \
   const uint32_t ci_=products.find(string_view(O_.s5[i_],(size_t)O_.l5[i_]));O_.uct[i_]|=(uint64_t)(ci_==KeyTable::NONE?0u:(unsigned)(uint16_t)ci_)<<32;} \
  if(__builtin_expect(ok_==0xFFu,1)){rows+=8; \
   vmx=_mm256_max_epu32(vmx,_mm256_max_epu32(_mm256_load_si256((const __m256i*)&O_.ve[0]),_mm256_load_si256((const __m256i*)&O_.ve[4])));fastAny=true; \
   SS_EV8_COMMIT8(O_) \
  }else{rows+=(unsigned)__builtin_popcount(m_); \
   for(unsigned r_=m_;r_;r_&=r_-1){const unsigned i_=(unsigned)__builtin_ctz(r_); \
    if((ok_>>i_)&1){send=max(send,(uint32_t)O_.ve[i_]+1);SS_EV8_COMMIT1(O_,i_)} \
    else SLOW(L_[i_],X_[i_]-1,((O_.idok>>i_)&1)?(uint32_t)O_.ve[i_]:NOPRE,lp,dmax,send,general);}} }
#define SS_EVENT_LOOP_EV8L(NAME,TARGET,LSCAN,LIVE,DEC,VNS,ECONST,SLOW) \
template<class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 if(p>=lim)return; \
 const char* const end=base+len; \
 const EvLoop lp(tl,tab,products,PE,end);uint32_t dmax=0;uint32_t send=0; \
 const VNS::EvConst ec=ECONST(lp); \
 VNS::Ev8Out out[2];VNS::Ev8Scan sc[2]; \
 alignas(64) const char* A[4][8];alignas(64) const char* le[8];const char* tail=nullptr; \
 {const char* cut_[9];lane_cut(p,lim,end,cut_);for(unsigned i_=0;i_<8;++i_){A[0][i_]=cut_[i_];le[i_]=cut_[i_+1];}} \
 const char* const* lD=A[0];const char* const* xD=A[0];const char* const* lF=A[0];const char* const* xF=A[0]; /* lines and next lines of D, F */ \
 unsigned g=0,nD=0,nF=0;VNS::Ev8Scan* sN=&sc[0];VNS::Ev8Scan* sD=&sc[1];VNS::Ev8Out* oD=&out[0];VNS::Ev8Out* oF=&out[1];bool more=true; /* row masks of D and F; N's and D's scan; D's and F's results */ \
 __m256i vmx=_mm256_setzero_si256();bool fastAny=false;uint64_t rows=0,shortG=0,fixed=0; \
 do{ \
  unsigned nN=0;const char** const lN=A[g&3];const char** const xN=A[(g+1)&3]; \
  if(more){ \
   const unsigned live_=LIVE(lN,le); \
   if(__builtin_expect(live_==0xFFu,1)){nN=0xFFu;const unsigned unc_=LSCAN(lN,xN,*sN);if(__builtin_expect(unc_!=0,0))nN&=~lane_fix(lN,xN,unc_,end,tail,fixed);SS_LANE_PF(xN)} \
   else if(live_){nN=live_;++shortG;const unsigned unc_=LSCAN(lN,xN,*sN)&live_;if(unc_)nN&=~lane_fix(lN,xN,unc_,end,tail,fixed); \
    for(unsigned i_=0;i_<8;++i_)if(!((live_>>i_)&1))xN[i_]=lN[i_];} /* a lane that is done stays where it is */ \
   else more=false; \
  } \
  if(nD){ \
   VNS::Ev8Out& o_=*oD; \
   DEC(lD,xD,*sD,ec,o_); \
   if(lp.strm){if(lp.spf)SS_EV8_SPF(o_,o_.idok&nD)} /* stream mode: no slot is written here, so none is asked for */ \
   else if(__builtin_expect((nD==0xFFu)&(o_.idok==0xFFu),1)){SS_EV8_PF(o_,0) SS_EV8_PF(o_,1) SS_EV8_PF(o_,2) SS_EV8_PF(o_,3) SS_EV8_PF(o_,4) SS_EV8_PF(o_,5) SS_EV8_PF(o_,6) SS_EV8_PF(o_,7)} \
   else for(unsigned x_=o_.idok&nD;x_;x_&=x_-1)SS_EV8_PF(o_,__builtin_ctz(x_)) \
  } \
  if(nF)SS_EV8L_FINISH(*oF,lF,xF,nF,SLOW) \
  lF=lD;xF=xD;nF=nD;lD=lN;xD=xN;nD=nN;++g; /* D becomes F, N becomes D */ \
  {VNS::Ev8Scan* const t_=sN;sN=sD;sD=t_;VNS::Ev8Out* const u_=oD;oD=oF;oF=u_;} \
 }while(more||nD||nF); \
 if(tail)line_general<7>(tail,end,general); /* the last line has no newline */ \
 if(fastAny){alignas(32) uint32_t v_[8];_mm256_store_si256((__m256i*)v_,vmx);send=max(send,max(max(v_[0],v_[2]),max(v_[4],v_[6]))+1);} \
 tl.denseMax=max(tl.denseMax,dmax);tl.evEnd=max(tl.evEnd,send);tl.ev8Rows+=rows;tl.laneShort+=shortG;tl.laneFixE+=fixed; \
}
// _e8l = the lane form of _e8 (kernel version 4 only). The avx2 instance is the test stand-in (scalar kernel).
SS_EVENT_LOOP_EV8L(events_avx2_e8l,SS_V3,lane_scan_ev_avx2,lane_live_scalar,ev8_dec_ref4,vert4,SS_EV8_CONST4,events_avx2_e8_slow)
SS_EVENT_LOOP_EV8L(events_avx512z_e8l,SS_V5,vlane::lscan8_v5,lane_live_avx512,vert4::dec8_v5,vert4,SS_EV8_CONST4,events_avx512z_e8_slow)
SS_EVENT_LOOP_EV8L(events_avx512y_e8l,SS_V5,vlane::lscan8_v5,lane_live_avx512,vert4::dec8_v5,vert4,SS_EV8_CONST4,events_avx512y_e8_slow)
// ---- general route, events: the ev8g kernel in lanes (GFAST.md packages A + B; agent EV) ----
// The frame is SS_EVENT_LOOP_EV8L: lanes, scan a group ahead of the decode, the commit a group behind it. What differs:
// the kernel is vertg (three windows, IDs as texts with a class), nothing is decoded as a counter, and the commit is
// the general route's: the user through the ID table, a PRec into the bucket of the event_id's hash.
//   D (decode)  + gev_ahead: per row hkey of the event_id (kept for F) and, for a valid event, the table slot of
//               the user asked for (packed key: hash_packed of the kernel's uk0 / uk1; canonical long: long_key's hash)
//   F (finish)  gev_finish: user number from that slot (find-or-add on a miss, exactly resolve_users / long_user),
//               category of a purchase, record appended. Rows the kernel refuses (ok bit clear) go through
//               event_line_v3<false,false> (and from there to the general handler if need be), one at a time.
// The user of a line that is not a valid event is not looked up (the line code does not either: the table would
// otherwise learn IDs the reference never saw). Rows are finished in lane order; the buckets do not care.
// SS_GEV=check: gev_check_row gives the line to the general handler as well (which then stores into tl.gcap instead of
// a bucket) and compares the 32 bytes of the two records, the exact ingest_ts and the line end.
// SS_ABL (TEST ONLY, WRONG OUTPUT: pricing on the scoring machine; the run stops after the PARSE mark): 1 nouser = no
// user lookup and no slot request (every row user 0); 2 norec = the row is decoded and resolved but its record is not
// stored (no bucket append / stream put, no fill-line request); 3 nokern = lane scan only: line starts found, nothing decoded.
static unsigned gevAbl=0;
static bool gevCheck=false,gevLongText=false; // gevLongText (SS_GEV_LONG=text): canonical long users through their text (long_user), as before
struct alignas(64) GevSide{uint64_t h[8];uint64_t hu[8];uint32_t j[8];}; // per row: hash of the event key; hash of a canonical long user; table slot of a packed user
template<class GEN> [[gnu::noinline]] static void gev_check_row(const char* l,const char* nx,const PRec* prec,uint64_t cls,uint64_t key,uint64_t hi,uint64_t k,uint32_t user,unsigned cat,int64_t ingest,const EvLoop& lp,GEN&& general){
 ThreadState& tl=lp.tl;
 const char* const nl=(const char*)memchr(l,'\n',(size_t)(lp.end-l));
 ThreadState::GCap cap;memset((void*)&cap,0,sizeof cap);tl.gcap=&cap;
 line_general<7>(l,lp.end,general);tl.gcap=nullptr;
 uint32_t cu;memcpy(&cu,(const void*)&cap.prec.user,4);
 if(nl&&nl+1==nx&&cap.has&&cap.cls==cls&&cap.key==key&&cap.hi==hi&&cap.prec.k==k&&cu==user&&cap.prec.cat==cat&&cap.ingest==ingest
    &&(!prec||memcmp((const void*)&cap.prec,(const void*)prec,sizeof(PRec))==0)){++tl.gevChecked;return;}
 const size_t n=nl?(size_t)(nl-l):(size_t)(lp.end-l);
 fprintf(stderr,"SS_GEV=check: row differs (line end %d, record %d, PRec %d): kernel class %llu key %llx hi %llx k %llu user %u cat %u ingest %lld | handler class %llu key %llx hi %llx k %llu user %u cat %u ingest %lld | line: %.*s\n",
  (int)(nl&&nl+1==nx),(int)cap.has,(int)(!prec||memcmp((const void*)&cap.prec,(const void*)prec,sizeof(PRec))==0),(unsigned long long)cls,(unsigned long long)key,(unsigned long long)hi,(unsigned long long)k,user,cat,(long long)ingest,
  (unsigned long long)cap.cls,(unsigned long long)cap.key,(unsigned long long)cap.hi,(unsigned long long)cap.prec.k,cu,(unsigned)cap.prec.cat,(long long)cap.ingest,(int)min<size_t>(n,300),l);
 abort();
}
// D: what F wants ready, for one ok row.
SS_V3I void gev_ahead_row(const vertg::Ev8gOut& o,GevSide& s,const unsigned i,const EvLoop& lp){
 if(lp.gstrm){ // the hash of the record's class; its bucket's fill line and state
  const uint64_t cls=o.ecls[i];
  const uint64_t h=__builtin_expect(cls==GCLS_SHORT,1)?gkey_hash_short(o.ekey[i]):cls==GCLS_LONG?gkey_hash_long(o.ekey[i],o.ehi[i]):hkey_v3(o.es[i],(size_t)o.el[i]);
  s.h[i]=h;
  if(lp.gspf&&gevAbl!=2){const size_t b=(size_t)(h>>lp.gsh);
   if(cls==GCLS_LONG){__builtin_prefetch(lp.gl->fill+b,1,3);__builtin_prefetch(lp.gl->bs+b,1,3);}
   else{__builtin_prefetch(lp.gs->fill+b*2,1,3);__builtin_prefetch(lp.gs->bs+b,1,3);}}
 }else s.h[i]=hkey_v3(o.es[i],(size_t)o.el[i]);
 if(o.kk[i]&&__builtin_expect(gevAbl!=1,1)){
  if(__builtin_expect(o.ucls[i]==0,1)){const size_t j=KeyTable::hash_packed(o.uk0[i],(uint32_t)o.uk1[i])&lp.tmask;s.j[i]=(uint32_t)j;__builtin_prefetch(&lp.slots[j],0,1);}
  else if(o.ucls[i]==1){const uint64_t hl=mix64(o.ulo[i]^mix64(o.uhi[i]^((uint64_t)(0x100u|(uint8_t)'u')*0x9E3779B97F4A7C15ull)));s.hu[i]=hl;__builtin_prefetch(&lp.slots[hl&lp.tmask],0,1);} // KeyTable::long_key's hash of 'u' + 32 hex
 }
}
// m = the rows of the group that are lines of this chunk. A whole group takes the loop without the row mask.
SS_V3I void gev_ahead(const vertg::Ev8gOut& o,GevSide& s,const unsigned m,const EvLoop& lp){
 const unsigned live=m&o.ok;
 if(__builtin_expect(live==0xFFu,1)){for(unsigned i=0;i<8;++i)gev_ahead_row(o,s,i,lp);}
 else for(unsigned x=live;x;x&=x-1)gev_ahead_row(o,s,(unsigned)__builtin_ctz(x),lp);
}
// F: one ok row becomes a record.
template<class GEN> SS_V3I void gev_finish_row(const vertg::Ev8gOut& o,const GevSide& s,const char* const* const L,const char* const* const X,const unsigned i,const EvLoop& lp,uint32_t* const pend,unsigned& cnt,GEN&& general){
 ThreadState& tl=lp.tl;KeyTable& tab=lp.tab;const auto R_=memory_order_relaxed;
 const uint64_t k=o.kk[i];uint32_t user=0;unsigned cat=0;
 if(k){
  if(__builtin_expect(gevAbl==1,0)){} // SS_ABL=nouser
  else if(__builtin_expect(o.ucls[i]==0,1)){
   const uint64_t k0=o.uk0[i];const uint32_t k1=(uint32_t)o.uk1[i];const size_t j=s.j[i];KeyTable::Slot& e=lp.slots[j];
   if(__builtin_expect(e.k1.load(memory_order_acquire)==k1&&e.k0.load(R_)==k0,1))user=e.v.load(R_);
   else{const string_view u(o.us[i],(size_t)o.ul[i]);user=tab.find_or_add_packed(k0,k1,j,u);if(user==KeyTable::NONE)user=LOCAL_USER|tl.users.intern(u);}
  }else if(o.ucls[i]==1&&!gevLongText){ // 'u' + 32 hex: the table's key of that text is (kind, lo, hi), which the kernel has (KeyTable::long_key)
   KeyTable::Long key;key.a=o.ulo[i];key.b=o.uhi[i];key.v=0;key.kind=0x100u|(uint8_t)'u';
   const string_view u(o.us[i],(size_t)o.ul[i]);user=tab.find_or_add_long_key(key,s.hu[i],u);if(user==KeyTable::NONE)user=LOCAL_USER|tl.users.intern(u);
  }else user=long_user(o.us[i],(size_t)o.ul[i],tab,tl);
  if((o.pur>>i)&1){const uint32_t ci=lp.products.find(string_view(o.s5[i],(size_t)o.l5[i]));cat=ci==KeyTable::NONE?0:(unsigned)(uint16_t)ci;}
 }
 const int64_t ingest=(int64_t)o.ingest[i];const uint64_t h=s.h[i];
 if(__builtin_expect(gevAbl==2,0)){lp.tl.gevAblSum+=k^h^user^cat^(uint64_t)ingest;return;} // SS_ABL=norec: the values are used, nothing is stored
 if(lp.gstrm){
  const uint64_t cls=o.ecls[i];
  if(__builtin_expect(gevCheck,0))gev_check_row(L[i],X[i],nullptr,cls,o.ekey[i],o.ehi[i],k,user,cat,ingest,lp,general);
  const size_t b=(size_t)(h>>lp.gsh);
  if(__builtin_expect(cls==GCLS_LONG,0))lp.gl->put(b,GRecL{o.ekey[i],o.ehi[i],k,grec_uc(user,cat,cls),ingest,{0,0,0}});
  else if(lp.gnote)lp.gs->put_noted(b,GRec{o.ekey[i],k,grec_uc(user,cat,cls),ingest},pend,cnt); // a full line leaves at the end of the group (no branch per row)
  else lp.gs->put(b,GRec{o.ekey[i],k,grec_uc(user,cat,cls),ingest});
  return;
 }
 const uint64_t code=ingest_code(ingest);
 if(__builtin_expect(code==INGEST_SIDE,0))tl.ingestSide.emplace_back(o.es[i],ingest); // out of the code's range: the exact value on the side, as the general handler does
 const PRec r=make_prec(o.es[i],h,code,k,cat,user);
 if(__builtin_expect(gevCheck,0))gev_check_row(L[i],X[i],&r,o.ecls[i],o.ekey[i],o.ehi[i],k,user,cat,ingest,lp,general);
 Bucket<PRec>& vec=lp.buckets[(size_t)(((h>>40)*lp.PE)>>24)];
 if(__builtin_expect(vec.full(),0))vec.reserve(max<size_t>(16,2*vec.capacity()));
 vec.append(r);
}
template<class GEN> SS_V3I void gev_finish(const vertg::Ev8gOut& o,const GevSide& s,const char* const* const L,const char* const* const X,const unsigned m,const EvLoop& lp,GEN&& general){
 const unsigned live=m&o.ok;uint32_t pend[8];unsigned cnt=0;
 if(__builtin_expect(live==0xFFu,1)){for(unsigned i=0;i<8;++i)gev_finish_row(o,s,L,X,i,lp,pend,cnt,general);}
 else for(unsigned x=live;x;x&=x-1)gev_finish_row(o,s,L,X,(unsigned)__builtin_ctz(x),lp,pend,cnt,general);
 lp.gs->send_noted(pend,cnt); // (nothing noted unless lp.gnote; before any other use of the store: the slow rows, the next group)
}
// The stand-in instance (scalar twin): no scan stage, every line end through lane_fix (memchr).
static inline unsigned gev_scan_none(const char* const*,const char**,vertg::Ev8gScan&){return 0xFFu;}
[[gnu::noinline]] static void gev_dec_ref(const char* const* ls,const char* const* nx,const vertg::Ev8gScan&,const vert4::EvConst& c,vertg::Ev8gOut& o){vertg::ev8g_ref(ls,nx,c,o);}
#define SS_EVENT_LOOP_G8L(NAME,TARGET,MASKS,LSCAN,LIVE,DEC) \
template<class GEN> TARGET [[gnu::noinline]] static void NAME##_slow(const char* const q,const EvLoop& lp,GEN&& general){ \
 PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0,send=0;unsigned rpos=0; \
 ++lp.tl.gevSlow; \
 if(lp.gstrm){line_general<7>(q,lp.end,general);return;} /* streams: the general handler resolves the user and puts the record */ \
 const WinMasks w1=MASKS(q);const char* const bb=event_window2(q,w1.c); \
 event_line_v3<false,false>(q,bb,w1,MASKS(bb),nullptr,0,lp,pend,pp,dmax,send,(ERing*)nullptr,rpos,general); \
 resolve_users(pend,pp,lp.tab,lp.tl); /* its record's user: before anything else is appended to that bucket */ \
} \
template<class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& products,size_t PE,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 if(p>=lim)return; \
 const char* const end=base+len; \
 const EvLoop lp(tl,tab,products,PE,end); \
 const vert4::EvConst ec=ev8_const4(0,0,lp.wstart,lp.wlen,0); \
 vertg::Ev8gOut out[2];vertg::Ev8gScan sc[2];GevSide side[2]; \
 alignas(64) const char* A[4][8];alignas(64) const char* le[8];const char* tail=nullptr; \
 {const char* cut_[9];lane_cut(p,lim,end,cut_);for(unsigned i_=0;i_<8;++i_){A[0][i_]=cut_[i_];le[i_]=cut_[i_+1];}} \
 const char* const* lD=A[0];const char* const* xD=A[0];const char* const* lF=A[0];const char* const* xF=A[0]; /* lines and next lines of D, F */ \
 unsigned g=0,nD=0,nF=0;vertg::Ev8gScan* sN=&sc[0];vertg::Ev8gScan* sD=&sc[1];vertg::Ev8gOut* oD=&out[0];vertg::Ev8gOut* oF=&out[1];GevSide* dD=&side[0];GevSide* dF=&side[1];bool more=true; \
 uint64_t rows=0,shortG=0,fixed=0; \
 do{ \
  unsigned nN=0;const char** const lN=A[g&3];const char** const xN=A[(g+1)&3]; \
  if(more){ \
   const unsigned live_=LIVE(lN,le); \
   if(__builtin_expect(live_==0xFFu,1)){nN=0xFFu;const unsigned unc_=LSCAN(lN,xN,*sN);if(__builtin_expect(unc_!=0,0))nN&=~lane_fix(lN,xN,unc_,end,tail,fixed);SS_LANE_PF(xN)} \
   else if(live_){nN=live_;++shortG;const unsigned unc_=LSCAN(lN,xN,*sN)&live_;if(unc_)nN&=~lane_fix(lN,xN,unc_,end,tail,fixed); \
    for(unsigned i_=0;i_<8;++i_)if(!((live_>>i_)&1))xN[i_]=lN[i_];} /* a lane that is done stays where it is */ \
   else more=false; \
  } \
  if(nD&&__builtin_expect(gevAbl!=3,1)){DEC(lD,xD,*sD,ec,*oD);gev_ahead(*oD,*dD,nD,lp);} \
  if(nF&&__builtin_expect(gevAbl==3,0)){rows+=(unsigned)__builtin_popcount(nF);} /* SS_ABL=nokern */ \
  else if(nF){rows+=(unsigned)__builtin_popcount(nF);gev_finish(*oF,*dF,lF,xF,nF,lp,general); \
   for(unsigned r_=nF&~(unsigned)oF->ok;__builtin_expect(r_!=0,0);r_&=r_-1)NAME##_slow(lF[__builtin_ctz(r_)],lp,general);} \
  lF=lD;xF=xD;nF=nD;lD=lN;xD=xN;nD=nN;++g; /* D becomes F, N becomes D */ \
  {vertg::Ev8gScan* const t_=sN;sN=sD;sD=t_;vertg::Ev8gOut* const u_=oD;oD=oF;oF=u_;GevSide* const v_=dD;dD=dF;dF=v_;} \
 }while(more||nD||nF); \
 if(tail)line_general<7>(tail,end,general); /* the last line has no newline */ \
 tl.gevRows+=rows;tl.laneShort+=shortG;tl.laneFixE+=fixed; \
}
SS_EVENT_LOOP_G8L(events_avx2_g8l,SS_V3,win_masks_avx2,gev_scan_none,lane_live_scalar,gev_dec_ref)
SS_EVENT_LOOP_G8L(events_avx512z_g8l,SS_V5,win_masks_avx512z,vertg::scang8_v5,lane_live_avx512,vertg::decg8_v5)
// The same for click lines (4 fields, one 64-byte window).
// A click line with no newline in its first window (GFAST.md "C": a long ID makes a line 64 to ~105 bytes): the fields
// from three windows, A at the line start (click_id up to the first comma), B one past that comma (user_id up to the
// second comma), C one past the second comma (campaign_id, ts and the newline). cA / qA: the comma and quote masks of
// window A, which holds no newline. False: the line is not four well-formed fields cut this way (a part longer than
// its window, a quote out of place, another number of commas): the general handler decides, as for any odd line.
// Up to 192 bytes behind p are read (the mappings leave 4096 zero bytes behind each file).
struct ClickLong{const char* s0;const char* next;unsigned l0,b1,l1,b2,l2,b3,l3;};
SS_V3I bool click_long_fields(const char* const p,const uint64_t cA,const uint64_t qA,ClickLong& f){
 if(!cA)return false;
 const unsigned c0=(unsigned)_tzcnt_u64(cA);
 uint64_t sqA,opA;const uint64_t errA=quote_errors_v3(qA&_blsmsk_u64(cA),1,_blsi_u64(cA),sqA,opA);
 const char* const bB=p+c0+1;
 const WinMasks wB=win_masks_avx2(bB);
 if(!wB.c)return false;
 const uint64_t mB=_blsmsk_u64(wB.c);
 const unsigned e1=(unsigned)_tzcnt_u64(wB.c);
 uint64_t sqB,opB;const uint64_t errB=quote_errors_v3(wB.q&mB,1,_blsi_u64(wB.c),sqB,opB);
 const char* const bC=bB+e1+1;
 const WinMasks wC=win_masks_avx2(bC);
 const uint64_t L=_tzcnt_u64(wC.n);
 if((wB.n&mB)|(L>>6))return false; // the line ends inside the user_id, or not inside window C
 const uint64_t nbit=_blsi_u64(wC.n),keep=nbit-1;
 const uint64_t c=wC.c&keep,q=wC.q&keep;
 const uint64_t st=(c<<1)|1,en=c|nbit;
 uint64_t sq,op;const uint64_t err=quote_errors_v3(q,st,en,sq,op);
 if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;
 const unsigned z0=(unsigned)(qA&1),z1=(unsigned)(wB.q&1);
 f.s0=p+z0;f.l0=c0-2*z0;
 f.b1=(unsigned)(bB-p)+z1;f.l1=e1-2*z1;
 uint64_t B=st+sq,E=en-(op>>1);
 const unsigned oC=(unsigned)(bC-p);
 const unsigned x2=(unsigned)_tzcnt_u64(B);f.b2=oC+x2;f.l2=(unsigned)_tzcnt_u64(E)-x2;
 B=_blsr_u64(B);E=_blsr_u64(E);
 const unsigned x3=(unsigned)_tzcnt_u64(B);f.b3=oC+x3;f.l3=(unsigned)_tzcnt_u64(E)-x3;
 f.next=bC+L+1;
 return true;
}
// SS_CLWIN=check: what click_long_fields said about the line at p against the portable splitter (scan_line +
// take_fields, the general handler's own reading). A line it took must be well-formed there with the same four
// fields and the same next line; a line it refused only has to be left to the general handler (which it is).
[[gnu::noinline]] static void click_long_check(const char* p,const char* end,bool fine,const ClickLong& f){
 if(!fine)return;
 const char* comma[4];int nc,nq;const char* le;string_view g[4];
 const char* const next=scan_line(p,end,comma,4,nc,nq,le);
 const bool gen=take_fields(p,le,comma,nc,nq,4,g);
 if(gen&&next==f.next&&g[0].data()==f.s0&&g[0].size()==f.l0&&g[1].data()==p+f.b1&&g[1].size()==f.l1
  &&g[2].data()==p+f.b2&&g[2].size()==f.l2&&g[3].data()==p+f.b3&&g[3].size()==f.l3)return;
 fprintf(stderr,"ERROR SS_CLWIN=check: long click line read differently: %.*s\n",(int)min<ptrdiff_t>(le-p,200),p);_exit(1);
}
struct ClLoop{ThreadState& tl;KeyTable& tab;const KeyTable& camps;size_t PC;const char* end;DateMemo3* memo;int64_t clickStart;uint64_t wlen;Bucket<CRec>* buckets;KeyTable::Slot* slots;size_t tmask;
 bool dUsers;uint64_t kc,ku,limC;CSlot* cl;uint32_t tid;bool bucket;unsigned ahead,shC;Bucket<DCRec>* dcb; // dense route (dparse)
 CampLook cf=campTable.look; // campaign lookup (camp_find)
 unsigned win2=optClickWin2; // lines without a newline in the first window: click_long_fields (SS_CLWIN=1: the general handler, as before)
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
   const char* s0;unsigned l0,b1,l1,b2,l2,b3,l3; // field contents (quotes stripped): s0 and offsets from p
   if(!DREC&&(L>>6)&&lp.win2){
    // No newline in the window (a line of 64 bytes or more: every line with a long ID): two more windows, B one
    // past the first comma (user_id up to the second comma) and C one past the second (campaign_id, ts, newline).
    // The same decisions as below, field by field; anything else leaves through the general handler as before.
    ClickLong f;const bool fine=click_long_fields(p,w.c,w.q,f);
    if(__builtin_expect(lp.win2==2,0))click_long_check(p,lp.end,fine,f);
    if(__builtin_expect(!fine,0))break;
    next=f.next;done=true;++tl.clWin2;
    s0=f.s0;l0=f.l0;b1=f.b1;l1=f.l1;b2=f.b2;l2=f.l2;b3=f.b3;l3=f.l3;
   }else{
   if(__builtin_expect((err|(uint64_t)(_mm_popcnt_u64(c)^3)|(L>>6))!=0,0))break;
   next=DREC?nl+1:p+L+1;done=true; // well-formed, 4 fields
   uint64_t B=st+sq,E=en-(op>>1);
   const unsigned z0=(unsigned)(q&1);
   s0=p+z0;l0=(unsigned)_tzcnt_u64(E)-z0;
   B=_blsr_u64(B);E=_blsr_u64(E);
   b1=(unsigned)_tzcnt_u64(B);l1=(unsigned)_tzcnt_u64(E)-b1;
   B=_blsr_u64(B);E=_blsr_u64(E);
   b2=(unsigned)_tzcnt_u64(B);l2=(unsigned)_tzcnt_u64(E)-b2;
   B=_blsr_u64(B);E=_blsr_u64(E);
   b3=(unsigned)_tzcnt_u64(B);l3=(unsigned)_tzcnt_u64(E)-b3;
   }
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
   // Copies of a click are identical, so validating before the dedup keeps the same clicks.
   int64_t ts=0;
   uint64_t trel;
   const bool tsOk=ts_v3(p+b3,l3,memo,ts);
   if(!tsOk||(trel=(uint64_t)ts-(uint64_t)clickStart)>=wlen)break; // clickStart <= ts < window_end
   uint64_t k0;uint32_t k1;
   uint32_t ci;if(!camp_find(lp.cf,camps,p+b2,l2,ci))break;
   const char* const s1=p+b1;
   // Dense route (DENSE.md): see event_line_v3.
   const uint64_t ckey=(trel<<16)|(uint16_t)ci;
   if(DREC){
    uint32_t user=ucode;
    if(__builtin_expect(uDense,1))dmax=max(dmax,ucode);
    else{ // a general user: through the ID table right here
     PendUser pu;
     if(__builtin_expect(!KeyTable::pack(string_view(s1,l1),pu.k0,pu.k1),0))user=long_user(s1,l1,tab,tl);
     else{pu.j=(uint32_t)(KeyTable::hash_packed(pu.k0,pu.k1)&tmask);pu.u=s1;pu.up=&user;
      resolve_users(&pu,&pu+1,tab,tl);}
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
    bool longUser=false; // a user ID that does not pack (over 11 bytes: every canonical long ID): waits in the batch like the others (SS_CLWIN=1: its number right here)
    if(tableUser&&__builtin_expect(!KeyTable::pack(string_view(s1,l1),k0,k1),0)){if(lp.win2)longUser=true;else{ucode=long_user(s1,l1,tab,tl);tableUser=false;}}
    CRec r;r.id=s0;r.idlen=l0;r.h=hkey_v3(s0,l0);r.key=ckey;r.user=ucode;
    Bucket<CRec>& vec=buckets[(size_t)(((r.h>>40)*PC)>>24)];
    if(__builtin_expect(vec.full(),0)){resolve_users(pend,pp,tab,tl);pp=pend;vec.reserve(max<size_t>(16,2*vec.capacity()));} // grow now: nothing waits on the old storage, and the record appended below stays put
    uint32_t* const up=&vec.e->user;vec.append(r);
    if(tableUser){
     if(__builtin_expect(longUser,0))pend_long_user(*pp++,s1,l1,tab,up);
     else{
      const size_t j=KeyTable::hash_packed(k0,k1)&tmask;
      __builtin_prefetch(&slots[j],0,1);
      PendUser& pu=*pp++;pu.k0=k0;pu.j=(uint32_t)j;pu.u=s1;pu.up=up;pu.k1=k1;
     }
     if(pp==pend+V3_BATCH){resolve_users(pend,pp,tab,tl);pp=pend;}
    }
   }
  }while(0);
  if(!done){
   resolve_users(pend,pp,tab,tl);pp=pend;++tl.clGeneral;
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
// ---- cl8 click loop (SS_CLICKS=cl8) ----
// The click analogue of the ev8 event loop: eight click lines at a time straight from the line index go to a kernel
// (vcl::cscan8_v5 + cdec8_v5, AVX-512; vcl::cl8_ref is the scalar twin with the same decisions, used by the avx2 loop
// as a test stand-in only). An ok row is what click_line_v3<true> would do up to its campaign lookup; the loop does the
// lookup and the commit. A drop row is a line the row code drops without a trace. Every other row goes through
// click_line_v3<true> unchanged, at its place in line order. Contract: /home/claude/vert/BATCH.md ("cl8") and the
// head of vert_cl.h. Slot form only (not the bucket form); the ring is not used (the distance from decode to
// finish is the prefetch lead: at least eight lines).
// ---- cl8: eight click lines per call (vertical AVX-512). Generated from /home/claude/vert/vert.h + vert_cl.h by mkpaste_cl.py;
// ---- contract at the top of vert_cl.h and in BATCH.md ("cl8: interface + integration"). Tested as this exact text by
// ---- vert_cl_test -DVERT_CL_PASTE. Do not edit here: change vert_cl.h and regenerate. Needs <immintrin.h> <cstdint> <cstring> <cstddef>.
namespace vcl{
#ifndef VERT_T5
#define VERT_T5 __attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512cd,avx2,bmi,bmi2,lzcnt,popcnt")))
#endif
static constexpr uint64_t MASK40=(1ull<<40)-1;
static constexpr uint64_t M1=0x9E3779B97F4Bull&MASK40,M2=0xD6E8FEB86659ull&MASK40;
static constexpr uint64_t inv40(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&MASK40;}
static constexpr uint64_t I1=inv40(M1),I2=inv40(M2);
static_assert(((M1*I1)&MASK40)==1&&((M2*I2)&MASK40)==1);
static constexpr int64_t DAYMS=86400000LL;
static constexpr uint64_t ESLOT_INVALID_K=2,EV_PURCHASE_BIT=1ull<<24,RAW_USER=0xC0000000u,PRICE_MAX=10000000; // main.cpp: ESLOT_INVALID, EV_PURCHASE, DENSE_USER_RAW, price_value's limit
static inline uint64_t index_of_ref(uint64_t y,uint64_t key){uint64_t m=(y*I2)&MASK40;m=m^(m>>17)^(m>>34);return ((m^key)*I1)&MASK40;}
static inline unsigned id8_ref(const uint64_t* w,const uint64_t* n,uint64_t key,uint64_t limit,uint64_t* ctr){
 unsigned acc=0;
 for(int i=0;i<8;++i){
  ctr[i]=0;
  const uint64_t len=n[i];
  if(len-1u>7u)continue;
  unsigned char s[8];memcpy(s,&w[i],8);
  if(s[0]=='0'&&len>1)continue;
  uint64_t v=0;bool ok=true;
  for(uint64_t j=0;j<len;++j){unsigned d=s[j]-(unsigned)'0';if(d>9){d=s[j]-(unsigned)'a';if(d>25){ok=false;break;}d+=10;}v=v*36+d;}
  if(!ok||v>MASK40)continue;
  const uint64_t c=index_of_ref(v,key);
  if(c<limit){ctr[i]=c;acc|=1u<<i;}
 }
 return acc;
}
static const uint64_t VKT[]={
 /* 0*/ (uint64_t)(64),
 /* 1*/ (uint64_t)(uint8_t)(0x30)*0x0101010101010101ULL,
 /* 2*/ (uint64_t)(uint8_t)('0')*0x0101010101010101ULL,
 /* 3*/ (uint64_t)(uint8_t)('a')*0x0101010101010101ULL,
 /* 4*/ (uint64_t)(uint8_t)(9)*0x0101010101010101ULL,
 /* 5*/ (uint64_t)(uint8_t)(25)*0x0101010101010101ULL,
 /* 6*/ (uint64_t)(uint8_t)((char)(10-'a'))*0x0101010101010101ULL,
 /* 7*/ (uint64_t)(uint16_t)(0x0124)*0x0001000100010001ULL,
 /* 8*/ (uint64_t)(uint32_t)(0x00010510)*0x0000000100000001ULL,
 /* 9*/ (uint64_t)(1679616),
 /*10*/ (uint64_t)(1),
 /*11*/ (uint64_t)(7),
 /*12*/ (uint64_t)(MASK40),
 /*13*/ (uint64_t)(I2),
 /*14*/ (uint64_t)(I1),
 /*15*/ (uint64_t)(0x0000000030540000ULL),
 /*16*/ (uint64_t)(0xFFFFFFFF0200FFFFULL),
 /*17*/ (uint64_t)(0x0030303A30303A30ULL),
 /*18*/ (uint64_t)(0xFF09050009050009ULL),
 /*19*/ (uint64_t)(0x0000000064000000ULL),
 /*20*/ (uint64_t)(0x0000000001680000ULL),
 /*21*/ (uint64_t)(0x00010A00010A0001ULL),
 /*22*/ (uint64_t)(0x00010001003C0E10ULL),
 /*23*/ (uint64_t)(1000),
 /*24*/ (uint64_t)(86400000),
 /*25*/ (uint64_t)(0x010A010A010A010AULL),
 /*26*/ (uint64_t)(0x0001006400010064ULL),
 /*27*/ (uint64_t)(0x010A010A01000000ULL),
 /*28*/ (uint64_t)(0x0001006400010000ULL),
 /*29*/ (uint64_t)(10000),
 /*30*/ (uint64_t)(100000),
 /*31*/ (uint64_t)(0x30303A3030000000ULL),
 /*32*/ (uint64_t)(0x0905000902FFFFFFULL),
 /*33*/ (uint64_t)(0x010A00010A000000ULL),
 /*34*/ (uint64_t)(0x0001003C003C0000ULL),
 /*35*/ (uint64_t)(60000),
 /*36*/ (uint64_t)(0x40),
 /*37*/ (uint64_t)(0x3C),
 /*38*/ (uint64_t)(0x0F03),
 /*39*/ (uint64_t)(0xFFFFFFFFULL),
 /*40*/ (uint64_t)(0xFFFF),
 /*41*/ (uint64_t)(4),
 /*42*/ (uint64_t)(0x77656976ULL),
 /*43*/ (uint64_t)(0x74726163ULL),
 /*44*/ (uint64_t)(8),
 /*45*/ (uint64_t)(0x6573616863727570ULL),
 /*46*/ (uint64_t)(2),
 /*47*/ (uint64_t)(3),
 /*48*/ (uint64_t)('Z'),
 /*49*/ (uint64_t)('+'),
 /*50*/ (uint64_t)('-'),
 /*51*/ (uint64_t)(20),
 /*52*/ (uint64_t)(25),
 /*53*/ (uint64_t)(13),
 /*54*/ (uint64_t)(DAYMS),
 /*55*/ (uint64_t)(uint8_t)(',')*0x0101010101010101ULL,
 /*56*/ (uint64_t)(uint8_t)('"')*0x0101010101010101ULL,
 /*57*/ (uint64_t)(-1),
 /*58*/ (uint64_t)(63),
 /*59*/ (uint64_t)(16),
 /*60*/ (uint64_t)(0xFF),
 /*61*/ (uint64_t)('e'),
 /*62*/ (uint64_t)('u'),
 /*63*/ (uint64_t)(9),
 /*64*/ (uint64_t)(12),
 /*65*/ (uint64_t)(5),
 /*66*/ (uint64_t)(17),
 /*67*/ (uint64_t)(PRICE_MAX-1),
 /*68*/ (uint64_t)(EV_PURCHASE_BIT),
 /*69*/ (uint64_t)(ESLOT_INVALID_K),
 /*70*/ (uint64_t)(RAW_USER),
};
static inline __attribute__((always_inline)) const uint64_t* vk_ptr(){const uint64_t* p=VKT;__asm__("":"+r"(p));return p;}
#define VERT_K5(i) _mm512_set1_epi64((long long)VK[i])
struct DateEnt{uint64_t k;uint32_t cc;uint32_t pad;int64_t ms;};
static_assert(sizeof(DateEnt)==24);
static inline unsigned date_slot(uint64_t w0,uint64_t w1){ // w0 = date bytes 0..7, w1 = bytes 8..15
 return (unsigned)(((w0>>34)&0x40)|((w0>>46)&0x3C)|(w1&0x0F03));
}
static inline unsigned char byte_of(uint64_t w,unsigned i){return (unsigned char)(w>>(8*i));}
static inline bool tod_ref(uint64_t w1,uint64_t w2,uint64_t& ms){
 if(byte_of(w1,2)!='T'||byte_of(w2,1)!=':'||byte_of(w2,4)!=':')return false;
 const unsigned ht=byte_of(w1,3)-(unsigned)'0',hu=byte_of(w2,0)-(unsigned)'0',mt=byte_of(w2,2)-(unsigned)'0',mu=byte_of(w2,3)-(unsigned)'0',st=byte_of(w2,5)-(unsigned)'0',su=byte_of(w2,6)-(unsigned)'0';
 if(ht>2||hu>9||mt>5||mu>9||st>5||su>9)return false;
 const uint64_t v=(uint64_t)(((ht*10+hu)*60+mt*10+mu)*60+st*10+su)*1000;
 if(v>=86400000ULL)return false;
 ms=v;return true;
}
static inline bool day_ref(uint64_t w0,uint64_t w1,const DateEnt* memo,uint64_t& dayms){
 const DateEnt& x=memo[date_slot(w0,w1)];
 if(x.k==((w0>>16)|(w1<<48))&&x.cc==(uint32_t)(w0&0xFFFF)){dayms=(uint64_t)x.ms;return true;}
 return false;
}
static inline bool epoch_ref(uint64_t w0,uint64_t w1,uint64_t& ms){
 uint64_t hi=0,lo=0;
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w0,i)-(unsigned)'0';if(d>9)return false;hi=hi*10+d;}
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w1,i)-(unsigned)'0';if(d>9)return false;if(i>=3)lo=lo*10+d;}
 ms=hi*100000+lo;return true;
}
static inline bool off_ref(uint64_t w2,uint64_t w3,int64_t& delta){
 const unsigned char sg=byte_of(w2,7);
 if((sg!='+'&&sg!='-')||byte_of(w3,5)!=':')return false;
 const unsigned o1=byte_of(w3,3)-(unsigned)'0',o2=byte_of(w3,4)-(unsigned)'0',o3=byte_of(w3,6)-(unsigned)'0',o4=byte_of(w3,7)-(unsigned)'0';
 if(o1>9||o2>9||o3>9||o4>9)return false;
 const unsigned oh=o1*10+o2,om=o3*10+o4;
 if(oh>23||om>59)return false; // (ts_v3 says "not a timestamp" here; the kernels simply refuse the row)
 const int64_t off=(int64_t)(oh*60+om)*60000LL;
 delta=sg=='-'?off:-off;return true;
}
#define VERT_C5(x) _mm512_set1_epi64((long long)(x))
#define VERT_LD5(p) _mm512_loadu_si512((const void*)(p))
VERT_T5 static inline __m512i tod8k_v5(__m512i a,__m512i b,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i va=_mm512_sub_epi8(a,VERT_K5(15)),la=VERT_K5(16);
 const __m512i vb=_mm512_sub_epi8(b,VERT_K5(17)),lb=VERT_K5(18);
 const __m512i pa=_mm512_madd_epi16(_mm512_maddubs_epi16(va,VERT_K5(19)),VERT_K5(20));
 const __m512i pb=_mm512_madd_epi16(_mm512_maddubs_epi16(vb,VERT_K5(21)),VERT_K5(22));
 const __m512i s=_mm512_add_epi32(pa,pb);
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(s,_mm512_srli_epi64(s,32)),VERT_K5(23));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(va,la),la),_mm512_max_epu8(vb,lb),lb),ms,VERT_K5(24));
 return ms;
}
VERT_T5 static inline __m512i epoch8k_v5(__m512i w0,__m512i w1,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i z=VERT_K5(2),nine=VERT_K5(4);
 const __m512i v0=_mm512_sub_epi8(w0,z),v1=_mm512_sub_epi8(w1,z);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v0,nine),nine),_mm512_max_epu8(v1,nine),nine);
 const __m512i p0=_mm512_madd_epi16(_mm512_maddubs_epi16(v0,VERT_K5(25)),VERT_K5(26));
 const __m512i p1=_mm512_madd_epi16(_mm512_maddubs_epi16(v1,VERT_K5(27)),VERT_K5(28));
 const __m512i tenk=VERT_K5(29);
 const __m512i hi=_mm512_add_epi64(_mm512_mul_epu32(p0,tenk),_mm512_srli_epi64(p0,32));
 const __m512i lo=_mm512_add_epi64(_mm512_mul_epu32(p1,tenk),_mm512_srli_epi64(p1,32));
 return _mm512_add_epi64(_mm512_mul_epu32(hi,VERT_K5(30)),lo);
}
VERT_T5 static inline __m512i off8k_v5(__m512i w3,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i v=_mm512_sub_epi8(w3,VERT_K5(31)),l=VERT_K5(32);
 const __m512i p=_mm512_madd_epi16(_mm512_maddubs_epi16(v,VERT_K5(33)),VERT_K5(34));
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(p,_mm512_srli_epi64(p,32)),VERT_K5(35));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v,l),l),ms,VERT_K5(24));
 return ms;
}
static inline uint64_t ld8_ref(const char* p){uint64_t w;memcpy(&w,p,8);return w;}
static inline void masks_ref(const char* p,uint64_t& c,uint64_t& q){c=0;q=0;for(int i=0;i<64;++i){c|=(uint64_t)(p[i]==',')<<i;q|=(uint64_t)(p[i]=='"')<<i;}}
static inline unsigned tz_ref(uint64_t x){return x?(unsigned)__builtin_ctzll(x):64u;}
static inline uint64_t blsr_ref(uint64_t x){return x&(x-1);}
static constexpr unsigned CL_GEN_ALIAS_BITS=9,CL_ALIAS_BITS=3; // main.cpp: dn::GEN_ALIAS_BITS, dn::ALIAS_BITS
static constexpr uint64_t DENSE_USER_BIT=0x40000000u;          // main.cpp: DENSE_USER
static constexpr unsigned CSLOT_SHIFT=4;                       // main.cpp: sizeof(CSlot) == 16
struct ClConst{uint64_t kc,limC,ku,limI;const uint64_t* cplx;int64_t clickStart;uint64_t wlen;const DateEnt* memo;};
struct alignas(64) Cl8Scan{uint64_t c[8],q[8];};
struct alignas(64) Cl8Out{
 uint64_t kcn[8];     // idok rows: click counter
 uint64_t so[8];      // idok rows: kcn << 4 (byte offset of the CSlot)
 uint64_t ckh[8];     // ok rows: (ts - clickStart) << 16; the caller ORs (uint16_t)campaign index
 uint64_t user[8];    // ok rows: DENSE_USER | index << 3 | alias
 const char* sc[8];uint64_t lc[8]; // ok rows: campaign_id content (quotes stripped)
 uint32_t umax,kend;  // over the ok rows: largest user, largest (uint32_t)kcn + 1 (0 if there is no ok row)
 uint8_t ok;          // bit i: the caller looks the campaign up and commits (see the contract)
 uint8_t drop;        // bit i: click_line_v3 drops the line without doing anything; never together with ok
 uint8_t idok;        // bit i: kcn[i] is the counter of a dense click_id on a line click_line_v3 decodes (ok implies idok)
};
// one row of K1 through id8_ref (the reference is allowed to be slow)
static inline bool id1_ref(uint64_t w,uint64_t n,uint64_t key,uint64_t limit,uint64_t& ctr){
 uint64_t out[8];const uint64_t ws[8]={w,w,w,w,w,w,w,w},ns[8]={n,n,n,n,n,n,n,n};
 if(!(id8_ref(ws,ns,key,limit,out)&1))return false;
 ctr=out[0];return true;
}
static inline void cl8_ref(const char* const* ls,const char* const* nx,const ClConst& c,Cl8Out& o){
 unsigned okm=0,idm=0,drm=0;uint32_t umax=0,kend=0;
 for(int i=0;i<8;++i){
  o.kcn[i]=o.so[i]=o.ckh[i]=o.user[i]=o.lc[i]=0;o.sc[i]=ls[i];
  const char* const p=ls[i];const char* const nl=nx[i]-1;
  uint64_t cm,qa;masks_ref(p,cm,qa);
  // structure, as click_line_v3 (DREC form: the newline is known)
  const uint64_t L=(uint64_t)(nl-p),nbit=1ull<<(L&63),keep=nbit-1;
  const uint64_t d=cm&keep,q=qa&keep;
  const uint64_t st=(d<<1)|1,en=d|nbit;
  const uint64_t sq=q&st,op=(sq+~en)&en,err=op^((q&~st)<<1);
  if((err|(uint64_t)(__builtin_popcountll(d)^3)|(L>>6))!=0)continue;      // not well-formed: the row code passes it on
  uint64_t B=st+sq,E=en-(op>>1);
  const unsigned z0=(unsigned)(q&1);
  const char* const s0=p+z0;const unsigned l0=tz_ref(E)-z0;
  B=blsr_ref(B);E=blsr_ref(E);
  const unsigned b1=tz_ref(B),l1=tz_ref(E)-b1;
  B=blsr_ref(B);E=blsr_ref(E);
  const unsigned b2=tz_ref(B),l2=tz_ref(E)-b2;
  B=blsr_ref(B);E=blsr_ref(E);
  const unsigned b3=tz_ref(B),l3=tz_ref(E)-b3;
  o.sc[i]=p+b2;o.lc[i]=l2;
  if(l0==0||l1==0){drm|=1u<<i;continue;}                                   // dropped before anything is decoded
  uint64_t kcn=0,uc=0;bool dCl=false,uD=false;uint32_t code=0;
  if(s0[0]=='k'&&id1_ref(ld8_ref(s0+1),(uint64_t)l0-1,c.kc,c.limC,kcn)){dCl=true;o.kcn[i]=kcn;o.so[i]=kcn<<CSLOT_SHIFT;idm|=1u<<i;}
  if(p[b1]=='u'&&id1_ref(ld8_ref(p+b1+1),(uint64_t)l1-1,c.ku,MASK40+1,uc)){ // dense_user_code
   const uint64_t idx=uc>>CL_GEN_ALIAS_BITS,alias=uc&((1u<<CL_GEN_ALIAS_BITS)-1);
   if(alias<(1u<<CL_ALIAS_BITS)&&idx<c.limI&&!((c.cplx[idx>>6]>>(idx&63))&1)){uD=true;code=(uint32_t)DENSE_USER_BIT|(uint32_t)((idx<<CL_ALIAS_BITS)|alias);}
  }
  // ts: the three fast forms of ts_v3; anything else is left to the row code
  const char* const s3=p+b3;
  int64_t ts;
  if(l3==13){uint64_t v;if(!epoch_ref(ld8_ref(s3),ld8_ref(s3+5),v))continue;ts=(int64_t)v;}
  else if(l3==20||l3==25){
   const uint64_t t0=ld8_ref(s3),t1=ld8_ref(s3+8),t2=ld8_ref(s3+12);
   uint64_t tods,days;
   if(!tod_ref(t1,t2,tods)||!day_ref(t0,t1,c.memo,days))continue;
   ts=(int64_t)(days+tods);
   if(l3==20){if(byte_of(t2,7)!='Z')continue;}
   else{int64_t delta;if(!off_ref(t2,ld8_ref(s3+17),delta))continue;ts+=delta;}
  }else continue;
  const uint64_t trel=(uint64_t)ts-(uint64_t)c.clickStart;
  if(trel>=c.wlen){drm|=1u<<i;continue;}                                   // outside the click window: dropped
  if(!dCl||!uD)continue;                                                   // bucket record / ID table: the row code's business
  okm|=1u<<i;o.ckh[i]=trel<<16;o.user[i]=code;
  if(code>umax)umax=code;
  if((uint32_t)kcn+1>kend)kend=(uint32_t)kcn+1;
 }
 o.ok=(uint8_t)okm;o.drop=(uint8_t)drm;o.idok=(uint8_t)idm;o.umax=umax;o.kend=kend;
}
// constants of the click kernel that the ev8 table (VKT) does not have; read through an opaque pointer for the same reason
static const uint64_t VKC[]={
 /*0*/ (uint64_t)('k'),
 /*1*/ (uint64_t)(((1u<<CL_GEN_ALIAS_BITS)-1)&~((1u<<CL_ALIAS_BITS)-1)), // alias bits that must be zero (alias < 8)
 /*2*/ (uint64_t)((1u<<CL_ALIAS_BITS)-1),
 /*3*/ (uint64_t)(0xFFFFFFFFu),
 /*4*/ (uint64_t)(DENSE_USER_BIT),
 /*5*/ (uint64_t)(0xFF),
};
static inline __attribute__((always_inline)) const uint64_t* vkc_ptr(){const uint64_t* p=VKC;__asm__("":"+r"(p));return p;}
// K1 as id8k_v5 of vert.h (same arithmetic, same answer), without its separate length test, which the other tests imply:
//   n == 0            x is eight '0' bytes, val = 0 < MIN36[8] = 36^7 (the entry picked by n & 7 == 0)   -> refused by val >= min36
//   n >= 9 (any size) both shift counts are 64 or more, x = 0, and a zero byte is not a base-36 digit      -> refused by bad != 0
VERT_T5 static inline __m512i cid8k_v5(__m512i W,__m512i N,uint64_t key,uint64_t limit,__mmask8& okOut){
 const uint64_t* const VK=vk_ptr();
 const __m512i n8=_mm512_slli_epi64(N,3);
 const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(W,_mm512_sub_epi64(VERT_K5(0),n8)),_mm512_srlv_epi64(VERT_K5(1),n8));
 const __m512i t=_mm512_sub_epi8(x,VERT_K5(2)),u=_mm512_sub_epi8(x,VERT_K5(3));
 const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(t,VERT_K5(4)),_mm512_subs_epu8(u,VERT_K5(5)));
 const __m512i d=_mm512_min_epu8(t,_mm512_add_epi8(x,VERT_K5(6)));
 const __m512i p2=_mm512_maddubs_epi16(d,VERT_K5(7));
 const __m512i p4=_mm512_madd_epi16(p2,VERT_K5(8));
 const __m512i val=_mm512_add_epi64(_mm512_mul_epu32(p4,VERT_K5(9)),_mm512_srli_epi64(p4,32));
 const __m512i min36=_mm512_permutexvar_epi64(N,_mm512_setr_epi64(78364164096LL,0,36,1296,46656,1679616,60466176,2176782336LL)); // MIN36[n] by the low three bits of n
 __mmask8 ok=_mm512_testn_epi64_mask(bad,bad);
 ok=_mm512_mask_cmpge_epu64_mask(ok,val,min36);
 ok=_mm512_mask_cmple_epu64_mask(ok,val,VERT_K5(12));
 const __m512i mask=VERT_K5(12);
 __m512i m=_mm512_and_si512(_mm512_mullo_epi64(val,VERT_K5(13)),mask);
 m=_mm512_ternarylogic_epi64(m,_mm512_srli_epi64(m,17),_mm512_srli_epi64(m,34),0x96);
 m=_mm512_xor_si512(m,_mm512_set1_epi64((long long)key));
 const __m512i c=_mm512_and_si512(_mm512_mullo_epi64(m,VERT_K5(14)),mask);
 okOut=_mm512_mask_cmplt_epu64_mask(ok,c,_mm512_set1_epi64((long long)limit));return c;
}
VERT_T5 static inline void cscan8_v5(const char* const* ls,Cl8Scan& s){
 const uint64_t* const VK=vk_ptr();
 const __m512i vc=VERT_K5(55),vq=VERT_K5(56);
 for(int i=0;i<8;++i){
  const __m512i x=_mm512_loadu_si512((const void*)ls[i]);
  s.c[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc));s.q[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq));
 }
}
// OPT (compile-time, identical results; the bench measures both):
//   bit 0  every gather gets zero as its pass-through operand. A gather merges into its destination register, so without
//          this it waits for whatever wrote that register last (an earlier gather, as a rule) although its mask is full.
//          Costs twelve register moves that take no execution port.
#ifndef VERT_CL8_OPT
#define VERT_CL8_OPT 1
#endif
template<unsigned OPT> VERT_T5 static inline void cdec8t_v5(const char* const* ls,const char* const* nx,const Cl8Scan& s,const ClConst& c,Cl8Out& o){
 const uint64_t* const VK=vk_ptr();const uint64_t* const VC=vkc_ptr();
 const __m512i one=VERT_K5(10),ones=VERT_K5(57),c63=VERT_K5(58),c64=VERT_K5(0);
 #define VERT_TZ5(x,xm) _mm512_sub_epi64(c63,_mm512_lzcnt_epi64(_mm512_xor_si512(x,xm))) /* tzcnt(x) given xm = x-1 (63 for x == 0) */
 // (OPT bit 0: the mask is all ones, but the compiler must not know - with a known full mask it drops the zero operand -
 // so it is read from the constant table, whose address the compiler cannot see through)
 const __mmask8 ALL=(OPT&1)?(__mmask8)VC[5]:(__mmask8)0xFF;
 #define VERT_GX5(i,b,sc) ((OPT&1)?_mm512_mask_i64gather_epi64(_mm512_setzero_si512(),ALL,i,b,sc):_mm512_i64gather_epi64(i,b,sc))
 #define VERT_G5(a) VERT_GX5(a,(const void*)0,1)
 #define VERT_KC5(i) _mm512_set1_epi64((long long)VC[i])
 const __m512i P=VERT_LD5(ls),NL=_mm512_add_epi64(VERT_LD5(nx),ones);
 const __m512i Cm=_mm512_load_si512((const void*)s.c),Qm=_mm512_load_si512((const void*)s.q);
 // structure: the bytes before the newline, three commas, quote rules (click_line_v3's test, lane-wise)
 const __m512i L=_mm512_sub_epi64(NL,P),nbit=_mm512_sllv_epi64(one,L),keep=_mm512_add_epi64(nbit,ones); // (a line of 64+ bytes gives nbit = 0: it fails the length test below whatever the masks are)
 const __m512i d=_mm512_and_si512(Cm,keep),qb=_mm512_and_si512(Qm,keep);
 const __m512i st=_mm512_or_si512(_mm512_slli_epi64(d,1),one),en=_mm512_or_si512(d,nbit);
 const __m512i sq=_mm512_and_si512(qb,st),op=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq,en),ones),en);
 const __m512i err=_mm512_xor_si512(op,_mm512_slli_epi64(_mm512_andnot_si512(st,qb),1));
 const __m512i u0=_mm512_add_epi64(d,ones),d0=VERT_TZ5(d,u0),dA=_mm512_and_si512(d,u0);
 const __m512i u1=_mm512_add_epi64(dA,ones),d1=VERT_TZ5(dA,u1),dB=_mm512_and_si512(dA,u1);
 const __m512i u2=_mm512_add_epi64(dB,ones),d2=VERT_TZ5(dB,u2),dR=_mm512_and_si512(dB,u2);
 const __m512i errAll=_mm512_or_si512(err,dR);                                  // a quote error, or a fourth comma before the newline
 // fields: a quoted field starts one later and is two shorter
 const __m512i z0=_mm512_and_si512(qb,one),l0=_mm512_sub_epi64(d0,_mm512_add_epi64(z0,z0));
 const __m512i a1=_mm512_add_epi64(d0,one),z1=_mm512_and_si512(_mm512_srlv_epi64(qb,a1),one),o1=_mm512_add_epi64(a1,z1),l1=_mm512_sub_epi64(_mm512_sub_epi64(d1,o1),z1);
 const __m512i a2=_mm512_add_epi64(d1,one),z2=_mm512_and_si512(_mm512_srlv_epi64(qb,a2),one),o2=_mm512_add_epi64(a2,z2),l2=_mm512_sub_epi64(_mm512_sub_epi64(d2,o2),z2);
 const __m512i a3=_mm512_add_epi64(d2,one),z3=_mm512_and_si512(_mm512_srlv_epi64(qb,a3),one),o3=_mm512_add_epi64(a3,z3),l3=_mm512_sub_epi64(_mm512_sub_epi64(L,o3),z3);
 // (the masks are combined as early as they can be: there are eight mask registers and every gather needs one)
 __mmask8 sok=_mm512_testn_epi64_mask(errAll,errAll);
 sok=_mm512_mask_test_epi64_mask(sok,dB,dB);                                    // at least three commas
 sok=_mm512_mask_cmplt_epu64_mask(sok,L,c64);                                   // newline inside the window (L >> 6 == 0)
 const __mmask8 nz=_mm512_mask_test_epi64_mask(_mm512_mask_test_epi64_mask(sok,l0,l0),l1,l1); // well-formed, click_id and user_id not empty
 const __mmask8 dropE=_kandn_mask8(nz,sok);                                     // well-formed with an empty ID
 const __m512i lowbyte=VERT_K5(60);
 // click_id: 'k' + K1 with kc, counter < limC
 const __m512i S0=_mm512_add_epi64(P,z0);
 __mmask8 idok;const __m512i kcn=cid8k_v5(VERT_G5(_mm512_add_epi64(S0,one)),_mm512_sub_epi64(l0,one),c.kc,c.limC,idok);
 idok=_kand_mask8(nz,_mm512_mask_cmpeq_epi64_mask(idok,_mm512_and_si512(VERT_G5(S0),lowbyte),VERT_KC5(0)));
 // user_id: 'u' + K1 with ku, then dense_user_code: alias < 8, index < limI, not complex
 const __m512i S1=_mm512_add_epi64(P,o1);
 const uint64_t limU=(c.limI<(1ull<<(40-CL_GEN_ALIAS_BITS))?c.limI:(1ull<<(40-CL_GEN_ALIAS_BITS)))<<CL_GEN_ALIAS_BITS; // index < limI  <=>  counter < limI << 9 (a counter is below 2^40)
 __mmask8 uok;const __m512i uc=cid8k_v5(VERT_G5(_mm512_add_epi64(S1,one)),_mm512_sub_epi64(l1,one),c.ku,limU,uok);
 uok=_mm512_mask_cmpeq_epi64_mask(uok,_mm512_and_si512(VERT_G5(S1),lowbyte),VERT_K5(62));
 uok=_mm512_mask_testn_epi64_mask(uok,uc,VERT_KC5(1));
 const __m512i cw=_mm512_mask_i64gather_epi64(_mm512_setzero_si512(),uok,_mm512_srli_epi64(uc,CL_GEN_ALIAS_BITS+6),(const void*)c.cplx,8); // only rows whose index is below limI are read
 uok=_mm512_mask_testn_epi64_mask(uok,cw,_mm512_rolv_epi64(one,_mm512_srli_epi64(uc,CL_GEN_ALIAS_BITS)));                               // bit (index & 63) of the word
 const __mmask8 okIU=_kand_mask8(idok,uok);
 __m512i user=_mm512_ternarylogic_epi64(VERT_KC5(2),uc,_mm512_srli_epi64(uc,CL_GEN_ALIAS_BITS-CL_ALIAS_BITS),0xCA);                      // index << 3 | alias (alias < 8)
 user=_mm512_ternarylogic_epi64(user,VERT_KC5(3),VERT_KC5(4),0xEA);                                                                      // DENSE_USER | (uint32_t)...
 // ts: 20 (Z), 13 (digits), 25 (offset), as in ev8
 const __mmask8 is13=_mm512_cmpeq_epi64_mask(l3,VERT_K5(53));
 const __m512i S3=_mm512_add_epi64(P,o3);
 const __m512i ts0=VERT_G5(S3),ts1=VERT_G5(_mm512_add_epi64(S3,_mm512_mask_mov_epi64(VERT_K5(44),is13,VERT_K5(65))));
 const __m512i ts2=VERT_G5(_mm512_add_epi64(S3,VERT_K5(64))),ts3=VERT_G5(_mm512_add_epi64(S3,VERT_K5(66)));
 __mmask8 oks,hits,okoff,okep;
 // the date through the memo (day8k_v5 of vert.h, with this function's gathers)
 __m512i days;
 {const __m512i slot=_mm512_ternarylogic_epi64(_mm512_and_si512(_mm512_srli_epi64(ts0,34),VERT_K5(36)),_mm512_and_si512(_mm512_srli_epi64(ts0,46),VERT_K5(37)),_mm512_and_si512(ts1,VERT_K5(38)),0xFE);
  const __m512i i3=_mm512_add_epi64(slot,_mm512_slli_epi64(slot,1));
  const long long* const mb=(const long long*)c.memo;
  const __m512i gk=VERT_GX5(i3,mb,8),gc=VERT_GX5(i3,mb+1,8);
  days=VERT_GX5(i3,mb+2,8);
  const __m512i dk=_mm512_or_si512(_mm512_srli_epi64(ts0,16),_mm512_slli_epi64(ts1,48));
  hits=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(gk,dk),_mm512_and_si512(gc,VERT_K5(39)),_mm512_and_si512(ts0,VERT_K5(40)));}
 const __m512i tods=tod8k_v5(ts1,ts2,oks);
 const __mmask8 dt=_kand_mask8(oks,hits);                                       // valid time of day and known date
 const __m512i ep=epoch8k_v5(ts0,ts1,okep);
 const __m512i base=_mm512_mask_mov_epi64(_mm512_add_epi64(days,tods),is13,ep);
 const __mmask8 t13=_kand_mask8(is13,okep);
 const __m512i b19=_mm512_srli_epi64(ts2,56);
 const __mmask8 tokA=_kor_mask8(t13,_mm512_mask_cmpeq_epi64_mask(_mm512_mask_cmpeq_epi64_mask(dt,l3,VERT_K5(51)),b19,VERT_K5(48))); // 13 digits, or 20 bytes ending in 'Z'
 const __m512i off=off8k_v5(ts3,okoff);
 const __mmask8 o25=_kand_mask8(dt,_mm512_mask_cmpeq_epi64_mask(okoff,l3,VERT_K5(52)));                                             // 25 bytes with a valid offset
 const __mmask8 p25=_mm512_mask_cmpeq_epi64_mask(o25,b19,VERT_K5(49));
 __m512i ts=_mm512_mask_sub_epi64(base,p25,base,off);
 const __mmask8 m25=_mm512_mask_cmpeq_epi64_mask(o25,b19,VERT_K5(50));
 ts=_mm512_mask_add_epi64(ts,m25,ts,off);
 // ts decided by the kernel: 13 digits, or (valid time and known date) and (Z form, or offset form with a valid offset)
 const __mmask8 tsk=_kand_mask8(nz,_kor_mask8(tokA,_kor_mask8(p25,m25)));
 const __m512i trel=_mm512_sub_epi64(ts,VERT_C5(c.clickStart));
 const __mmask8 inw=_mm512_mask_cmplt_epu64_mask(tsk,trel,VERT_C5(c.wlen));
 const __mmask8 ok=_kand_mask8(inw,okIU);
 const __mmask8 drop=_kor_mask8(dropE,_kandn_mask8(inw,tsk));                   // an empty ID, or ts outside the window
 _mm512_store_si512((void*)o.kcn,kcn);_mm512_store_si512((void*)o.so,_mm512_slli_epi64(kcn,CSLOT_SHIFT));
 _mm512_store_si512((void*)o.ckh,_mm512_slli_epi64(trel,16));
 _mm512_store_si512((void*)o.user,user);
 _mm512_store_si512((void*)o.sc,_mm512_add_epi64(P,o2));_mm512_store_si512((void*)o.lc,l2);
 // maxima over the ok rows, both in one pass of 32-bit lanes: low dword = user, high dword = (uint32_t)kcn + 1
 {const __m512i pk=_mm512_maskz_or_epi64(ok,user,_mm512_slli_epi64(_mm512_add_epi64(kcn,one),32));
  const __m256i m4=_mm256_max_epu32(_mm512_castsi512_si256(pk),_mm512_extracti64x4_epi64(pk,1));
  __m128i m2=_mm_max_epu32(_mm256_castsi256_si128(m4),_mm256_extracti128_si256(m4,1));
  m2=_mm_max_epu32(m2,_mm_unpackhi_epi64(m2,m2));
  _mm_storel_epi64((__m128i*)(void*)&o.umax,m2);}
 o.ok=(uint8_t)ok;o.drop=(uint8_t)drop;o.idok=(uint8_t)idok;
 #undef VERT_KC5
 #undef VERT_G5
 #undef VERT_GX5
 #undef VERT_TZ5
}
VERT_T5 static inline void cdec8_v5(const char* const* ls,const char* const* nx,const Cl8Scan& s,const ClConst& c,Cl8Out& o){cdec8t_v5<VERT_CL8_OPT>(ls,nx,s,c,o);}
VERT_T5 static inline void cl8_v5(const char* const* ls,const char* const* nx,const ClConst& c,Cl8Out& o){
 Cl8Scan s;cscan8_v5(ls,s);cdec8_v5(ls,nx,s,c,o);
}
static_assert(offsetof(Cl8Out,kend)==offsetof(Cl8Out,umax)+4);
#undef VERT_C5
#undef VERT_LD5
#undef VERT_K5
} // namespace vcl
static_assert(sizeof(vcl::DateEnt)==sizeof(DateMemo3)&&offsetof(vcl::DateEnt,k)==offsetof(DateMemo3,k)&&offsetof(vcl::DateEnt,cc)==offsetof(DateMemo3,cc)&&offsetof(vcl::DateEnt,ms)==offsetof(DateMemo3,ms));
static_assert(vcl::MASK40==dn::MASK&&vcl::I1==dn::I1&&vcl::I2==dn::I2&&vcl::DAYMS==DAY);
static_assert(vcl::CL_GEN_ALIAS_BITS==dn::GEN_ALIAS_BITS&&vcl::CL_ALIAS_BITS==dn::ALIAS_BITS&&vcl::DENSE_USER_BIT==DENSE_USER&&sizeof(CSlot)==(1u<<vcl::CSLOT_SHIFT));
// Version 2 of the click kernel (namespace vcl2, /home/claude/vert/cl8_paste2.h pasted unchanged): no gather instructions
// and no date memo; the dates it decides are the year-months of the run's click window (ClConst.d from make_dates_cl).
// It is the default kernel of the cl8 loop; SS_CL8=1 picks version 1 above (same loop, same results).
// ---- cl8 VERSION 2: eight click lines per call (vertical AVX-512, no gather instructions, no date memo). Generated from
// ---- /home/claude/vert/vert.h + vert_cl.h by mkpaste_cl2.py; contract in BATCH.md ("cl8 version 2"). Tested as this exact
// ---- text by vert_cl_test2 -DVERT_CL_PASTE. Do not edit here: change vert_cl.h and regenerate.
// ---- Caller: vcl2::ClConst cc{lp.kc, lp.limC, lp.ku, dense.limI, denseComplex, lp.clickStart, lp.wlen, nullptr};
// ----         cc.d = vcl2::make_dates_cl(lp.clickStart, window_end, window_start);   then exactly as namespace vcl (version 1).
namespace vcl2{
#ifndef VERT_T5
#define VERT_T5 __attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512cd,avx2,bmi,bmi2,lzcnt,popcnt")))
#endif
static constexpr uint64_t MASK40=(1ull<<40)-1;
static constexpr uint64_t M1=0x9E3779B97F4Bull&MASK40,M2=0xD6E8FEB86659ull&MASK40;
static constexpr uint64_t inv40(uint64_t a){uint64_t x=a;for(int i=0;i<6;++i)x*=2-a*x;return x&MASK40;}
static constexpr uint64_t I1=inv40(M1),I2=inv40(M2);
static_assert(((M1*I1)&MASK40)==1&&((M2*I2)&MASK40)==1);
static constexpr int64_t DAYMS=86400000LL;
static constexpr uint64_t ESLOT_INVALID_K=2,EV_PURCHASE_BIT=1ull<<24,RAW_USER=0xC0000000u,PRICE_MAX=10000000; // main.cpp: ESLOT_INVALID, EV_PURCHASE, DENSE_USER_RAW, price_value's limit
static inline uint64_t index_of_ref(uint64_t y,uint64_t key){uint64_t m=(y*I2)&MASK40;m=m^(m>>17)^(m>>34);return ((m^key)*I1)&MASK40;}
static inline unsigned id8_ref(const uint64_t* w,const uint64_t* n,uint64_t key,uint64_t limit,uint64_t* ctr){
 unsigned acc=0;
 for(int i=0;i<8;++i){
  ctr[i]=0;
  const uint64_t len=n[i];
  if(len-1u>7u)continue;
  unsigned char s[8];memcpy(s,&w[i],8);
  if(s[0]=='0'&&len>1)continue;
  uint64_t v=0;bool ok=true;
  for(uint64_t j=0;j<len;++j){unsigned d=s[j]-(unsigned)'0';if(d>9){d=s[j]-(unsigned)'a';if(d>25){ok=false;break;}d+=10;}v=v*36+d;}
  if(!ok||v>MASK40)continue;
  const uint64_t c=index_of_ref(v,key);
  if(c<limit){ctr[i]=c;acc|=1u<<i;}
 }
 return acc;
}
static constexpr uint64_t VKT[]={
 /* 0*/ (uint64_t)(64),
 /* 1*/ (uint64_t)(uint8_t)(0x30)*0x0101010101010101ULL,
 /* 2*/ (uint64_t)(uint8_t)('0')*0x0101010101010101ULL,
 /* 3*/ (uint64_t)(uint8_t)('a')*0x0101010101010101ULL,
 /* 4*/ (uint64_t)(uint8_t)(9)*0x0101010101010101ULL,
 /* 5*/ (uint64_t)(uint8_t)(25)*0x0101010101010101ULL,
 /* 6*/ (uint64_t)(uint8_t)((char)(10-'a'))*0x0101010101010101ULL,
 /* 7*/ (uint64_t)(uint16_t)(0x0124)*0x0001000100010001ULL,
 /* 8*/ (uint64_t)(uint32_t)(0x00010510)*0x0000000100000001ULL,
 /* 9*/ (uint64_t)(1679616),
 /*10*/ (uint64_t)(1),
 /*11*/ (uint64_t)(7),
 /*12*/ (uint64_t)(MASK40),
 /*13*/ (uint64_t)(I2),
 /*14*/ (uint64_t)(I1),
 /*15*/ (uint64_t)(0x0000000030540000ULL),
 /*16*/ (uint64_t)(0xFFFFFFFF0200FFFFULL),
 /*17*/ (uint64_t)(0x0030303A30303A30ULL),
 /*18*/ (uint64_t)(0xFF09050009050009ULL),
 /*19*/ (uint64_t)(0x0000000064000000ULL),
 /*20*/ (uint64_t)(0x0000000001680000ULL),
 /*21*/ (uint64_t)(0x00010A00010A0001ULL),
 /*22*/ (uint64_t)(0x00010001003C0E10ULL),
 /*23*/ (uint64_t)(1000),
 /*24*/ (uint64_t)(86400000),
 /*25*/ (uint64_t)(0x010A010A010A010AULL),
 /*26*/ (uint64_t)(0x0001006400010064ULL),
 /*27*/ (uint64_t)(0x010A010A01000000ULL),
 /*28*/ (uint64_t)(0x0001006400010000ULL),
 /*29*/ (uint64_t)(10000),
 /*30*/ (uint64_t)(100000),
 /*31*/ (uint64_t)(0x30303A3030000000ULL),
 /*32*/ (uint64_t)(0x0905000902FFFFFFULL),
 /*33*/ (uint64_t)(0x010A00010A000000ULL),
 /*34*/ (uint64_t)(0x0001003C003C0000ULL),
 /*35*/ (uint64_t)(60000),
 /*36*/ (uint64_t)(0x40),
 /*37*/ (uint64_t)(0x3C),
 /*38*/ (uint64_t)(0x0F03),
 /*39*/ (uint64_t)(0xFFFFFFFFULL),
 /*40*/ (uint64_t)(0xFFFF),
 /*41*/ (uint64_t)(4),
 /*42*/ (uint64_t)(0x77656976ULL),
 /*43*/ (uint64_t)(0x74726163ULL),
 /*44*/ (uint64_t)(8),
 /*45*/ (uint64_t)(0x6573616863727570ULL),
 /*46*/ (uint64_t)(2),
 /*47*/ (uint64_t)(3),
 /*48*/ (uint64_t)('Z'),
 /*49*/ (uint64_t)('+'),
 /*50*/ (uint64_t)('-'),
 /*51*/ (uint64_t)(20),
 /*52*/ (uint64_t)(25),
 /*53*/ (uint64_t)(13),
 /*54*/ (uint64_t)(DAYMS),
 /*55*/ (uint64_t)(uint8_t)(',')*0x0101010101010101ULL,
 /*56*/ (uint64_t)(uint8_t)('"')*0x0101010101010101ULL,
 /*57*/ (uint64_t)(-1),
 /*58*/ (uint64_t)(63),
 /*59*/ (uint64_t)(16),
 /*60*/ (uint64_t)(0xFF),
 /*61*/ (uint64_t)('e'),
 /*62*/ (uint64_t)('u'),
 /*63*/ (uint64_t)(9),
 /*64*/ (uint64_t)(12),
 /*65*/ (uint64_t)(5),
 /*66*/ (uint64_t)(17),
 /*67*/ (uint64_t)(PRICE_MAX-1),
 /*68*/ (uint64_t)(EV_PURCHASE_BIT),
 /*69*/ (uint64_t)(ESLOT_INVALID_K),
 /*70*/ (uint64_t)(RAW_USER),
 /*71*/ (uint64_t)(0x3030),
 /*72*/ (uint64_t)(0xFFFFFFFFFFFF0909ULL),
 /*73*/ (uint64_t)(0x010A),
 /*74*/ (uint64_t)(128),
};
#ifndef VERT_K64
#define VERT_K64 0
#endif
#if VERT_K64
struct VKWide{alignas(64) uint64_t w[sizeof(VKT)/sizeof(VKT[0])*8];constexpr VKWide():w{}{for(unsigned i=0;i<sizeof(VKT)/sizeof(VKT[0]);++i)for(unsigned j=0;j<8;++j)w[8*i+j]=VKT[i];}};
static constexpr VKWide VKW{};
static inline __attribute__((always_inline)) const uint64_t* vk_ptr(){const uint64_t* p=VKW.w;__asm__("":"+r"(p));return p;}
#define VERT_K5(i) _mm512_load_si512((const void*)(VK+8*(i)))
#else
static inline __attribute__((always_inline)) const uint64_t* vk_ptr(){const uint64_t* p=VKT;__asm__("":"+r"(p));return p;}
#define VERT_K5(i) _mm512_set1_epi64((long long)VK[i])
#endif
struct DateEnt{uint64_t k;uint32_t cc;uint32_t pad;int64_t ms;};
static_assert(sizeof(DateEnt)==24);
static constexpr unsigned ND=4; // version 4: at most this many year-months have dates the kernel accepts
struct EvDates{uint64_t pfx[ND];uint64_t base[ND];uint64_t dim[ND];uint64_t n;}; // "YYYY-MM-" as 8 bytes, ms of day 1 at 00:00Z, days of the month; n used (0: no date is accepted)
static inline unsigned char byte_of(uint64_t w,unsigned i){return (unsigned char)(w>>(8*i));}
static inline bool tod_ref(uint64_t w1,uint64_t w2,uint64_t& ms){
 if(byte_of(w1,2)!='T'||byte_of(w2,1)!=':'||byte_of(w2,4)!=':')return false;
 const unsigned ht=byte_of(w1,3)-(unsigned)'0',hu=byte_of(w2,0)-(unsigned)'0',mt=byte_of(w2,2)-(unsigned)'0',mu=byte_of(w2,3)-(unsigned)'0',st=byte_of(w2,5)-(unsigned)'0',su=byte_of(w2,6)-(unsigned)'0';
 if(ht>2||hu>9||mt>5||mu>9||st>5||su>9)return false;
 const uint64_t v=(uint64_t)(((ht*10+hu)*60+mt*10+mu)*60+st*10+su)*1000;
 if(v>=86400000ULL)return false;
 ms=v;return true;
}
static inline bool epoch_ref(uint64_t w0,uint64_t w1,uint64_t& ms){
 uint64_t hi=0,lo=0;
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w0,i)-(unsigned)'0';if(d>9)return false;hi=hi*10+d;}
 for(unsigned i=0;i<8;++i){const unsigned d=byte_of(w1,i)-(unsigned)'0';if(d>9)return false;if(i>=3)lo=lo*10+d;}
 ms=hi*100000+lo;return true;
}
static inline bool off_ref(uint64_t w2,uint64_t w3,int64_t& delta){
 const unsigned char sg=byte_of(w2,7);
 if((sg!='+'&&sg!='-')||byte_of(w3,5)!=':')return false;
 const unsigned o1=byte_of(w3,3)-(unsigned)'0',o2=byte_of(w3,4)-(unsigned)'0',o3=byte_of(w3,6)-(unsigned)'0',o4=byte_of(w3,7)-(unsigned)'0';
 if(o1>9||o2>9||o3>9||o4>9)return false;
 const unsigned oh=o1*10+o2,om=o3*10+o4;
 if(oh>23||om>59)return false; // (ts_v3 says "not a timestamp" here; the kernels simply refuse the row)
 const int64_t off=(int64_t)(oh*60+om)*60000LL;
 delta=sg=='-'?off:-off;return true;
}
#define VERT_C5(x) _mm512_set1_epi64((long long)(x))
#define VERT_LD5(p) _mm512_loadu_si512((const void*)(p))
VERT_T5 static inline __m512i tod8k_v5(__m512i a,__m512i b,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i va=_mm512_sub_epi8(a,VERT_K5(15)),la=VERT_K5(16);
 const __m512i vb=_mm512_sub_epi8(b,VERT_K5(17)),lb=VERT_K5(18);
 const __m512i pa=_mm512_madd_epi16(_mm512_maddubs_epi16(va,VERT_K5(19)),VERT_K5(20));
 const __m512i pb=_mm512_madd_epi16(_mm512_maddubs_epi16(vb,VERT_K5(21)),VERT_K5(22));
 const __m512i s=_mm512_add_epi32(pa,pb);
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(s,_mm512_srli_epi64(s,32)),VERT_K5(23));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(va,la),la),_mm512_max_epu8(vb,lb),lb),ms,VERT_K5(24));
 return ms;
}
VERT_T5 static inline __m512i epoch8k_v5(__m512i w0,__m512i w1,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i z=VERT_K5(2),nine=VERT_K5(4);
 const __m512i v0=_mm512_sub_epi8(w0,z),v1=_mm512_sub_epi8(w1,z);
 ok=_mm512_mask_cmpeq_epi64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v0,nine),nine),_mm512_max_epu8(v1,nine),nine);
 const __m512i p0=_mm512_madd_epi16(_mm512_maddubs_epi16(v0,VERT_K5(25)),VERT_K5(26));
 const __m512i p1=_mm512_madd_epi16(_mm512_maddubs_epi16(v1,VERT_K5(27)),VERT_K5(28));
 const __m512i tenk=VERT_K5(29);
 const __m512i hi=_mm512_add_epi64(_mm512_mul_epu32(p0,tenk),_mm512_srli_epi64(p0,32));
 const __m512i lo=_mm512_add_epi64(_mm512_mul_epu32(p1,tenk),_mm512_srli_epi64(p1,32));
 return _mm512_add_epi64(_mm512_mul_epu32(hi,VERT_K5(30)),lo);
}
VERT_T5 static inline __m512i off8k_v5(__m512i w3,__mmask8& ok){
 const uint64_t* const VK=vk_ptr();
 const __m512i v=_mm512_sub_epi8(w3,VERT_K5(31)),l=VERT_K5(32);
 const __m512i p=_mm512_madd_epi16(_mm512_maddubs_epi16(v,VERT_K5(33)),VERT_K5(34));
 const __m512i ms=_mm512_mul_epu32(_mm512_add_epi32(p,_mm512_srli_epi64(p,32)),VERT_K5(35));
 ok=_mm512_mask_cmplt_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(v,l),l),ms,VERT_K5(24));
 return ms;
}
static inline uint64_t ld8_ref(const char* p){uint64_t w;memcpy(&w,p,8);return w;}
static inline void masks_ref(const char* p,uint64_t& c,uint64_t& q){c=0;q=0;for(int i=0;i<64;++i){c|=(uint64_t)(p[i]==',')<<i;q|=(uint64_t)(p[i]=='"')<<i;}}
static inline unsigned tz_ref(uint64_t x){return x?(unsigned)__builtin_ctzll(x):64u;}
static inline uint64_t blsr_ref(uint64_t x){return x&(x-1);}
static inline bool day4_ref(uint64_t w0,uint64_t w1,const EvDates& d,uint64_t& dayms){
 const unsigned a=byte_of(w1,0)-(unsigned)'0',b=byte_of(w1,1)-(unsigned)'0';
 if(a>9||b>9)return false;
 const uint64_t t=(uint64_t)(a*10+b)-1; // day - 1 (day 0 wraps and fails the test below)
 for(uint64_t j=0;j<d.n&&j<ND;++j)if(w0==d.pfx[j]){if(t>=d.dim[j])return false;dayms=d.base[j]+t*(uint64_t)DAYMS;return true;}
 return false;
}
VERT_T5 static inline __m512i day4k_v5(__m512i w0,__m512i w1,const EvDates& d,__mmask8& hit){
 const uint64_t* const VK=vk_ptr();
 const __m512i vd=_mm512_sub_epi8(w1,VERT_K5(71)),lim=VERT_K5(72);
 const __m512i t=_mm512_sub_epi64(_mm512_maddubs_epi16(vd,VERT_K5(73)),VERT_K5(10)); // day - 1 (bytes 2..7 are multiplied by 0)
 __m512i base=_mm512_setzero_si512(),dim=_mm512_setzero_si512();                      // no prefix: dim 0, nothing is below it
 const uint64_t n=d.n<ND?d.n:ND;
 for(uint64_t j=0;j<n;++j){
  const __mmask8 m=_mm512_cmpeq_epi64_mask(w0,_mm512_set1_epi64((long long)d.pfx[j]));
  base=_mm512_mask_mov_epi64(base,m,_mm512_set1_epi64((long long)d.base[j]));
  dim=_mm512_mask_mov_epi64(dim,m,_mm512_set1_epi64((long long)d.dim[j]));
 }
 hit=_mm512_mask_cmplt_epu64_mask(_mm512_cmpeq_epi64_mask(_mm512_max_epu8(vd,lim),lim),t,dim); // two digits, 1 <= day <= days of the month
 return _mm512_add_epi64(base,_mm512_mul_epu32(t,VERT_K5(24)));
}
VERT_T5 static inline __attribute__((always_inline)) void ld16x8_v5(const uint64_t* a,long off,__m512i& lo,__m512i& hi){
 #define VERT_X5(i) _mm_loadu_si128((const __m128i*)((const char*)a[i]+off))
 __m512i e=_mm512_castsi128_si512(VERT_X5(0)),q=_mm512_castsi128_si512(VERT_X5(1));
 e=_mm512_inserti32x4(e,VERT_X5(2),1);q=_mm512_inserti32x4(q,VERT_X5(3),1);
 e=_mm512_inserti32x4(e,VERT_X5(4),2);q=_mm512_inserti32x4(q,VERT_X5(5),2);
 e=_mm512_inserti32x4(e,VERT_X5(6),3);q=_mm512_inserti32x4(q,VERT_X5(7),3);
 #undef VERT_X5
 lo=_mm512_unpacklo_epi64(e,q);hi=_mm512_unpackhi_epi64(e,q);
}
VERT_T5 static inline __attribute__((always_inline)) void ld16x8p_v5(const char* const* a,__m512i& lo,__m512i& hi){
 #define VERT_X5(i) _mm_loadu_si128((const __m128i*)a[i])
 __m512i e=_mm512_castsi128_si512(VERT_X5(0)),q=_mm512_castsi128_si512(VERT_X5(1));
 e=_mm512_inserti32x4(e,VERT_X5(2),1);q=_mm512_inserti32x4(q,VERT_X5(3),1);
 e=_mm512_inserti32x4(e,VERT_X5(4),2);q=_mm512_inserti32x4(q,VERT_X5(5),2);
 e=_mm512_inserti32x4(e,VERT_X5(6),3);q=_mm512_inserti32x4(q,VERT_X5(7),3);
 #undef VERT_X5
 lo=_mm512_unpacklo_epi64(e,q);hi=_mm512_unpackhi_epi64(e,q);
}
static constexpr unsigned CL_GEN_ALIAS_BITS=9,CL_ALIAS_BITS=3; // main.cpp: dn::GEN_ALIAS_BITS, dn::ALIAS_BITS
static constexpr uint64_t DENSE_USER_BIT=0x40000000u;          // main.cpp: DENSE_USER
static constexpr unsigned CSLOT_SHIFT=4;                       // main.cpp: sizeof(CSlot) == 16
struct ClConst{uint64_t kc,limC,ku,limI;const uint64_t* cplx;int64_t clickStart;uint64_t wlen;const DateEnt* memo;EvDates d={};}; // memo: version 1 only; d (= make_dates_cl(...)): version 2 only
struct alignas(64) Cl8Scan{uint64_t c[8],q[8];};
struct alignas(64) Cl8Out{
 uint64_t kcn[8];     // idok rows: click counter
 uint64_t so[8];      // idok rows: kcn << 4 (byte offset of the CSlot)
 uint64_t ckh[8];     // ok rows: (ts - clickStart) << 16; the caller ORs (uint16_t)campaign index
 uint64_t user[8];    // ok rows: DENSE_USER | index << 3 | alias
 const char* sc[8];uint64_t lc[8]; // ok rows: campaign_id content (quotes stripped)
 uint32_t umax,kend;  // over the ok rows: largest user, largest (uint32_t)kcn + 1 (0 if there is no ok row)
 uint8_t ok;          // bit i: the caller looks the campaign up and commits (see the contract)
 uint8_t drop;        // bit i: click_line_v3 drops the line without doing anything; never together with ok
 uint8_t idok;        // bit i: kcn[i] is the counter of a dense click_id on a line click_line_v3 decodes (ok implies idok)
};
static inline bool id1_ref(uint64_t w,uint64_t n,uint64_t key,uint64_t limit,uint64_t& ctr){
 uint64_t out[8];const uint64_t ws[8]={w,w,w,w,w,w,w,w},ns[8]={n,n,n,n,n,n,n,n};
 if(!(id8_ref(ws,ns,key,limit,out)&1))return false;
 ctr=out[0];return true;
}
static const uint64_t VKC[]={
 /*0*/ (uint64_t)('k'),
 /*1*/ (uint64_t)(((1u<<CL_GEN_ALIAS_BITS)-1)&~((1u<<CL_ALIAS_BITS)-1)), // alias bits that must be zero (alias < 8)
 /*2*/ (uint64_t)((1u<<CL_ALIAS_BITS)-1),
 /*3*/ (uint64_t)(0xFFFFFFFFu),
 /*4*/ (uint64_t)(DENSE_USER_BIT),
 /*5*/ (uint64_t)(0xFF),
};
static inline __attribute__((always_inline)) const uint64_t* vkc_ptr(){const uint64_t* p=VKC;__asm__("":"+r"(p));return p;}
VERT_T5 static inline __m512i cid8k_v5(__m512i W,__m512i N,uint64_t key,uint64_t limit,__mmask8& okOut){
 const uint64_t* const VK=vk_ptr();
 const __m512i n8=_mm512_slli_epi64(N,3);
 const __m512i x=_mm512_or_si512(_mm512_sllv_epi64(W,_mm512_sub_epi64(VERT_K5(0),n8)),_mm512_srlv_epi64(VERT_K5(1),n8));
 const __m512i t=_mm512_sub_epi8(x,VERT_K5(2)),u=_mm512_sub_epi8(x,VERT_K5(3));
 const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(t,VERT_K5(4)),_mm512_subs_epu8(u,VERT_K5(5)));
 const __m512i d=_mm512_min_epu8(t,_mm512_add_epi8(x,VERT_K5(6)));
 const __m512i p2=_mm512_maddubs_epi16(d,VERT_K5(7));
 const __m512i p4=_mm512_madd_epi16(p2,VERT_K5(8));
 const __m512i val=_mm512_add_epi64(_mm512_mul_epu32(p4,VERT_K5(9)),_mm512_srli_epi64(p4,32));
 const __m512i min36=_mm512_permutexvar_epi64(N,_mm512_setr_epi64(78364164096LL,0,36,1296,46656,1679616,60466176,2176782336LL)); // MIN36[n] by the low three bits of n
 __mmask8 ok=_mm512_testn_epi64_mask(bad,bad);
 ok=_mm512_mask_cmpge_epu64_mask(ok,val,min36);
 ok=_mm512_mask_cmple_epu64_mask(ok,val,VERT_K5(12));
 const __m512i mask=VERT_K5(12);
 __m512i m=_mm512_and_si512(_mm512_mullo_epi64(val,VERT_K5(13)),mask);
 m=_mm512_ternarylogic_epi64(m,_mm512_srli_epi64(m,17),_mm512_srli_epi64(m,34),0x96);
 m=_mm512_xor_si512(m,_mm512_set1_epi64((long long)key));
 const __m512i c=_mm512_and_si512(_mm512_mullo_epi64(m,VERT_K5(14)),mask);
 okOut=_mm512_mask_cmplt_epu64_mask(ok,c,_mm512_set1_epi64((long long)limit));return c;
}
VERT_T5 static inline void cscan8_v5(const char* const* ls,Cl8Scan& s){
 const uint64_t* const VK=vk_ptr();
 const __m512i vc=VERT_K5(55),vq=VERT_K5(56);
 for(int i=0;i<8;++i){
  const __m512i x=_mm512_loadu_si512((const void*)ls[i]);
  s.c[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc));s.q[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq));
 }
}
static inline EvDates make_dates_cl(int64_t click_start,int64_t window_end,int64_t window_start){
 EvDates r={};
 if(window_end<click_start)return r;
 const int64_t ingestBase=(int64_t)((uint64_t)window_start-(1ull<<38)); // as run() sets it: date3's range rule
 const auto fl=[](int64_t a){return a>=0?a/DAYMS:-((-a+DAYMS-1)/DAYMS);};
 int64_t ly=-1;unsigned lm=0;
 for(int64_t day=fl(click_start)-3;day<=fl(window_end)+3&&r.n<ND;++day){
  const int64_t z=day+719468;const int64_t era=(z>=0?z:z-146096)/146097;const unsigned doe=(unsigned)(z-era*146097);
  const unsigned yoe=(doe-doe/1460+doe/36524-doe/146096)/365;int64_t y=(int64_t)yoe+era*400;
  const unsigned doy=doe-(365*yoe+yoe/4-yoe/100),mp=(5*doy+2)/153,d=doy-(153*mp+2)/5+1,m=mp<10?mp+3:mp-9;y+=m<=2;
  if(y<0||y>9999)continue;
  if(y==ly&&m==lm)continue;
  ly=y;lm=m;
  static constexpr unsigned mlen[13]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  const unsigned dim=mlen[m]+((m==2&&((y%4==0&&y%100!=0)||y%400==0))?1u:0u);
  const int64_t first=(day-(int64_t)(d-1))*DAYMS,last=first+(int64_t)(dim-1)*DAYMS; // day 1 and the last day of this month
  if(first<ingestBase||(uint64_t)last-(uint64_t)ingestBase>=((1ull<<39)-1)-(uint64_t)DAYMS)continue;
  unsigned char t[8];
  t[0]=(unsigned char)('0'+y/1000);t[1]=(unsigned char)('0'+y/100%10);t[2]=(unsigned char)('0'+y/10%10);t[3]=(unsigned char)('0'+y%10);t[4]='-';
  t[5]=(unsigned char)('0'+m/10);t[6]=(unsigned char)('0'+m%10);t[7]='-';
  uint64_t w;memcpy(&w,t,8);
  r.pfx[r.n]=w;r.base[r.n]=(uint64_t)first;r.dim[r.n]=dim;++r.n;
 }
 return r;
}
static inline void cl8_ref(const char* const* ls,const char* const* nx,const ClConst& c,Cl8Out& o){
 unsigned okm=0,idm=0,drm=0;uint32_t umax=0,kend=0;
 for(int i=0;i<8;++i){
  o.kcn[i]=o.so[i]=o.ckh[i]=o.user[i]=o.lc[i]=0;o.sc[i]=ls[i];
  const char* const p=ls[i];const char* const nl=nx[i]-1;
  uint64_t cm,qa;masks_ref(p,cm,qa);
  // structure, as click_line_v3 (DREC form: the newline is known)
  const uint64_t L=(uint64_t)(nl-p),nbit=1ull<<(L&63),keep=nbit-1;
  const uint64_t d=cm&keep,q=qa&keep;
  const uint64_t st=(d<<1)|1,en=d|nbit;
  const uint64_t sq=q&st,op=(sq+~en)&en,err=op^((q&~st)<<1);
  if((err|(uint64_t)(__builtin_popcountll(d)^3)|(L>>6))!=0)continue;      // not well-formed: the row code passes it on
  uint64_t B=st+sq,E=en-(op>>1);
  const unsigned z0=(unsigned)(q&1);
  const char* const s0=p+z0;const unsigned l0=tz_ref(E)-z0;
  B=blsr_ref(B);E=blsr_ref(E);
  const unsigned b1=tz_ref(B),l1=tz_ref(E)-b1;
  B=blsr_ref(B);E=blsr_ref(E);
  const unsigned b2=tz_ref(B),l2=tz_ref(E)-b2;
  B=blsr_ref(B);E=blsr_ref(E);
  const unsigned b3=tz_ref(B),l3=tz_ref(E)-b3;
  o.sc[i]=p+b2;o.lc[i]=l2;
  if(l0==0||l1==0){drm|=1u<<i;continue;}                                   // dropped before anything is decoded
  uint64_t kcn=0,uc=0;bool dCl=false,uD=false;uint32_t code=0;
  if(s0[0]=='k'&&id1_ref(ld8_ref(s0+1),(uint64_t)l0-1,c.kc,c.limC,kcn)){dCl=true;o.kcn[i]=kcn;o.so[i]=kcn<<CSLOT_SHIFT;idm|=1u<<i;}
  if(p[b1]=='u'&&id1_ref(ld8_ref(p+b1+1),(uint64_t)l1-1,c.ku,MASK40+1,uc)){ // dense_user_code
   const uint64_t idx=uc>>CL_GEN_ALIAS_BITS,alias=uc&((1u<<CL_GEN_ALIAS_BITS)-1);
   if(alias<(1u<<CL_ALIAS_BITS)&&idx<c.limI&&!((c.cplx[idx>>6]>>(idx&63))&1)){uD=true;code=(uint32_t)DENSE_USER_BIT|(uint32_t)((idx<<CL_ALIAS_BITS)|alias);}
  }
  // ts: the three fast forms of ts_v3; anything else is left to the row code
  const char* const s3=p+b3;
  int64_t ts;
  if(l3==13){uint64_t v;if(!epoch_ref(ld8_ref(s3),ld8_ref(s3+5),v))continue;ts=(int64_t)v;}
  else if(l3==20||l3==25){
   const uint64_t t0=ld8_ref(s3),t1=ld8_ref(s3+8),t2=ld8_ref(s3+12);
   uint64_t tods,days;
   if(!tod_ref(t1,t2,tods)||!day4_ref(t0,t1,c.d,days))continue;
   ts=(int64_t)(days+tods);
   if(l3==20){if(byte_of(t2,7)!='Z')continue;}
   else{int64_t delta;if(!off_ref(t2,ld8_ref(s3+17),delta))continue;ts+=delta;}
  }else continue;
  const uint64_t trel=(uint64_t)ts-(uint64_t)c.clickStart;
  if(trel>=c.wlen){drm|=1u<<i;continue;}                                   // outside the click window: dropped
  if(!dCl||!uD)continue;                                                   // bucket record / ID table: the row code's business
  okm|=1u<<i;o.ckh[i]=trel<<16;o.user[i]=code;
  if(code>umax)umax=code;
  if((uint32_t)kcn+1>kend)kend=(uint32_t)kcn+1;
 }
 o.ok=(uint8_t)okm;o.drop=(uint8_t)drm;o.idok=(uint8_t)idm;o.umax=umax;o.kend=kend;
}
VERT_T5 static inline void cdec8_v5(const char* const* ls,const char* const* nx,const Cl8Scan& s,const ClConst& c,Cl8Out& o){
 const uint64_t* const VK=vk_ptr();const uint64_t* const VC=vkc_ptr();
 const __m512i one=VERT_K5(10),ones=VERT_K5(57),c63=VERT_K5(58),c64=VERT_K5(0);
 #define VERT_TZ5(x,xm) _mm512_sub_epi64(c63,_mm512_lzcnt_epi64(_mm512_xor_si512(x,xm))) /* tzcnt(x) given xm = x-1 (63 for x == 0) */
 #define VERT_KC5(i) _mm512_set1_epi64((long long)VC[i])
 const __m512i P=VERT_LD5(ls),NL=_mm512_add_epi64(VERT_LD5(nx),ones);
 const __m512i Cm=_mm512_load_si512((const void*)s.c),Qm=_mm512_load_si512((const void*)s.q);
 // structure: the bytes before the newline, three commas, quote rules (the text of cdec8t_v5)
 const __m512i L=_mm512_sub_epi64(NL,P),nbit=_mm512_sllv_epi64(one,L),keep=_mm512_add_epi64(nbit,ones); // (a line of 64+ bytes gives nbit = 0: it fails the length test below whatever the masks are)
 const __m512i d=_mm512_and_si512(Cm,keep),qb=_mm512_and_si512(Qm,keep);
 const __m512i st=_mm512_or_si512(_mm512_slli_epi64(d,1),one),en=_mm512_or_si512(d,nbit);
 const __m512i sq=_mm512_and_si512(qb,st),op=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq,en),ones),en);
 const __m512i err=_mm512_xor_si512(op,_mm512_slli_epi64(_mm512_andnot_si512(st,qb),1));
 const __m512i u0=_mm512_add_epi64(d,ones),d0=VERT_TZ5(d,u0),dA=_mm512_and_si512(d,u0);
 const __m512i u1=_mm512_add_epi64(dA,ones),d1=VERT_TZ5(dA,u1),dB=_mm512_and_si512(dA,u1);
 const __m512i u2=_mm512_add_epi64(dB,ones),d2=VERT_TZ5(dB,u2),dR=_mm512_and_si512(dB,u2);
 const __m512i errAll=_mm512_or_si512(err,dR);                                  // a quote error, or a fourth comma before the newline
 // fields: a quoted field starts one later and is two shorter
 const __m512i z0=_mm512_and_si512(qb,one),l0=_mm512_sub_epi64(d0,_mm512_add_epi64(z0,z0));
 const __m512i a1=_mm512_add_epi64(d0,one),z1=_mm512_and_si512(_mm512_srlv_epi64(qb,a1),one),o1=_mm512_add_epi64(a1,z1),l1=_mm512_sub_epi64(_mm512_sub_epi64(d1,o1),z1);
 const __m512i a2=_mm512_add_epi64(d1,one),z2=_mm512_and_si512(_mm512_srlv_epi64(qb,a2),one),o2=_mm512_add_epi64(a2,z2),l2=_mm512_sub_epi64(_mm512_sub_epi64(d2,o2),z2);
 const __m512i a3=_mm512_add_epi64(d2,one),z3=_mm512_and_si512(_mm512_srlv_epi64(qb,a3),one),o3=_mm512_add_epi64(a3,z3),l3=_mm512_sub_epi64(_mm512_sub_epi64(L,o3),z3);
 __mmask8 sok=_mm512_testn_epi64_mask(errAll,errAll);
 sok=_mm512_mask_test_epi64_mask(sok,dB,dB);                                    // at least three commas
 sok=_mm512_mask_cmplt_epu64_mask(sok,L,c64);                                   // newline inside the window (L >> 6 == 0)
 const __mmask8 nz=_mm512_mask_test_epi64_mask(_mm512_mask_test_epi64_mask(sok,l0,l0),l1,l1); // well-formed, click_id and user_id not empty
 const __mmask8 dropE=_kandn_mask8(nz,sok);                                     // well-formed with an empty ID
 const __m512i lowbyte=VERT_K5(60);
 // ---- operand fetch without gathers (user and ts start at most 65 bytes into the line: every load stays below line start + 96) ----
 alignas(64) uint64_t AD[16];
 _mm512_store_si512((void*)AD,_mm512_add_epi64(P,o1));_mm512_store_si512((void*)(AD+8),_mm512_add_epi64(P,o3));
 const uint64_t* ad=AD;__asm__("":"+r"(ad)::"memory"); // the reads below are scalar loads from memory, not extracts from the vectors
 __m512i LO,HI,U0,U1,ts0,tsh,ts2,tsx;
 ld16x8p_v5(ls,LO,HI);           // line bytes 0..15 (click_id)
 ld16x8_v5(ad,0,U0,U1);          // user: letter + the 15 bytes after it
 ld16x8_v5(ad+8,0,ts0,tsh);      // ts bytes 0..15
 ld16x8_v5(ad+8,12,ts2,tsx);     // ts bytes 12..27
 // click_id: 'k' + K1 with kc, counter < limC (letter = line byte z0, digits = line bytes z0+1 .. z0+8)
 const __m512i z8=_mm512_slli_epi64(z0,3),i8=_mm512_add_epi64(z8,VERT_K5(44));
 const __m512i E0=_mm512_srlv_epi64(LO,z8);
 const __m512i IDW=_mm512_or_si512(_mm512_srlv_epi64(LO,i8),_mm512_sllv_epi64(HI,_mm512_sub_epi64(c64,i8)));
 __mmask8 idok;const __m512i kcn=cid8k_v5(IDW,_mm512_sub_epi64(l0,one),c.kc,c.limC,idok);
 idok=_kand_mask8(nz,_mm512_mask_cmpeq_epi64_mask(idok,_mm512_and_si512(E0,lowbyte),VERT_KC5(0)));
 // user_id: 'u' + K1 with ku, then dense_user_code: alias < 8, index < limI, not complex
 const uint64_t limU=(c.limI<(1ull<<(40-CL_GEN_ALIAS_BITS))?c.limI:(1ull<<(40-CL_GEN_ALIAS_BITS)))<<CL_GEN_ALIAS_BITS; // index < limI  <=>  counter < limI << 9 (a counter is below 2^40)
 const __m512i UW=_mm512_or_si512(_mm512_srli_epi64(U0,8),_mm512_slli_epi64(U1,56)); // the 8 bytes after the letter
 __mmask8 uok;const __m512i uc=cid8k_v5(UW,_mm512_sub_epi64(l1,one),c.ku,limU,uok);
 uok=_mm512_mask_cmpeq_epi64_mask(uok,_mm512_and_si512(U0,lowbyte),VERT_K5(62));
 uok=_mm512_mask_testn_epi64_mask(uok,uc,VERT_KC5(1));
 if(!_kortestz_mask8_u8(uok,uok)){ // the complex bit, in general registers; only when some row is a candidate (then word 0 exists)
  alignas(64) uint64_t IX[8];
  _mm512_store_si512((void*)IX,_mm512_maskz_srli_epi64(uok,uc,CL_GEN_ALIAS_BITS)); // the user index; 0 for a row that is not a candidate
  const uint64_t* ix=IX;__asm__("":"+r"(ix)::"memory");
  const uint64_t* const cp=c.cplx;unsigned cb=0;
  for(int i=0;i<8;++i){uint64_t x=ix[i];__asm__("":"+r"(x));cb|=(unsigned)((cp[x>>6]>>(x&63))&1)<<i;} // (the empty asm keeps the loop in general registers: the compiler would vectorize it)
  uok=_kandn_mask8((__mmask8)cb,uok);
 }
 const __mmask8 okIU=_kand_mask8(idok,uok);
 __m512i user=_mm512_ternarylogic_epi64(VERT_KC5(2),uc,_mm512_srli_epi64(uc,CL_GEN_ALIAS_BITS-CL_ALIAS_BITS),0xCA);                      // index << 3 | alias (alias < 8)
 user=_mm512_ternarylogic_epi64(user,VERT_KC5(3),VERT_KC5(4),0xEA);                                                                      // DENSE_USER | (uint32_t)...
 // ts: 20 (Z), 13 (digits), 25 (offset), as in ev8
 const __mmask8 is13=_mm512_cmpeq_epi64_mask(l3,VERT_K5(53));
 const __m512i ts1=_mm512_mask_or_epi64(tsh,is13,_mm512_srli_epi64(ts0,40),_mm512_slli_epi64(tsh,24)); // bytes 8..15, for 13 digits bytes 5..12
 const __m512i ts3=_mm512_or_si512(_mm512_srli_epi64(ts2,40),_mm512_slli_epi64(tsx,24));               // bytes 17..24
 __mmask8 oks,hits,okoff,okep;
 const __m512i days=day4k_v5(ts0,ts1,c.d,hits);
 const __m512i tods=tod8k_v5(ts1,ts2,oks);
 const __mmask8 dt=_kand_mask8(oks,hits);                                       // valid time of day and known date
 const __m512i ep=epoch8k_v5(ts0,ts1,okep);
 const __m512i base=_mm512_mask_mov_epi64(_mm512_add_epi64(days,tods),is13,ep);
 const __mmask8 t13=_kand_mask8(is13,okep);
 const __m512i b19=_mm512_srli_epi64(ts2,56);
 const __mmask8 tokA=_kor_mask8(t13,_mm512_mask_cmpeq_epi64_mask(_mm512_mask_cmpeq_epi64_mask(dt,l3,VERT_K5(51)),b19,VERT_K5(48))); // 13 digits, or 20 bytes ending in 'Z'
 const __m512i off=off8k_v5(ts3,okoff);
 const __mmask8 o25=_kand_mask8(dt,_mm512_mask_cmpeq_epi64_mask(okoff,l3,VERT_K5(52)));                                             // 25 bytes with a valid offset
 const __mmask8 p25=_mm512_mask_cmpeq_epi64_mask(o25,b19,VERT_K5(49));
 __m512i ts=_mm512_mask_sub_epi64(base,p25,base,off);
 const __mmask8 m25=_mm512_mask_cmpeq_epi64_mask(o25,b19,VERT_K5(50));
 ts=_mm512_mask_add_epi64(ts,m25,ts,off);
 // ts decided by the kernel: 13 digits, or (valid time and known date) and (Z form, or offset form with a valid offset)
 const __mmask8 tsk=_kand_mask8(nz,_kor_mask8(tokA,_kor_mask8(p25,m25)));
 const __m512i trel=_mm512_sub_epi64(ts,VERT_C5(c.clickStart));
 const __mmask8 inw=_mm512_mask_cmplt_epu64_mask(tsk,trel,VERT_C5(c.wlen));
 const __mmask8 ok=_kand_mask8(inw,okIU);
 const __mmask8 drop=_kor_mask8(dropE,_kandn_mask8(inw,tsk));                   // an empty ID, or ts outside the window
 _mm512_store_si512((void*)o.kcn,kcn);_mm512_store_si512((void*)o.so,_mm512_slli_epi64(kcn,CSLOT_SHIFT));
 _mm512_store_si512((void*)o.ckh,_mm512_slli_epi64(trel,16));
 _mm512_store_si512((void*)o.user,user);
 _mm512_store_si512((void*)o.sc,_mm512_add_epi64(P,o2));_mm512_store_si512((void*)o.lc,l2);
 // maxima over the ok rows, both in one pass of 32-bit lanes: low dword = user, high dword = (uint32_t)kcn + 1
 {const __m512i pk=_mm512_maskz_or_epi64(ok,user,_mm512_slli_epi64(_mm512_add_epi64(kcn,one),32));
  const __m256i m4=_mm256_max_epu32(_mm512_castsi512_si256(pk),_mm512_extracti64x4_epi64(pk,1));
  __m128i m2=_mm_max_epu32(_mm256_castsi256_si128(m4),_mm256_extracti128_si256(m4,1));
  m2=_mm_max_epu32(m2,_mm_unpackhi_epi64(m2,m2));
  _mm_storel_epi64((__m128i*)(void*)&o.umax,m2);}
 o.ok=(uint8_t)ok;o.drop=(uint8_t)drop;o.idok=(uint8_t)idok;
 #undef VERT_KC5
 #undef VERT_TZ5
}
VERT_T5 static inline void cl8_v5(const char* const* ls,const char* const* nx,const ClConst& c,Cl8Out& o){
 Cl8Scan s;cscan8_v5(ls,s);cdec8_v5(ls,nx,s,c,o);
}
static_assert(offsetof(Cl8Out,kend)==offsetof(Cl8Out,umax)+4);
#undef VERT_C5
#undef VERT_LD5
#undef VERT_K5
} // namespace vcl2
static_assert(vcl2::MASK40==dn::MASK&&vcl2::I1==dn::I1&&vcl2::I2==dn::I2&&vcl2::DAYMS==DAY);
static_assert(vcl2::CL_GEN_ALIAS_BITS==dn::GEN_ALIAS_BITS&&vcl2::CL_ALIAS_BITS==dn::ALIAS_BITS&&vcl2::DENSE_USER_BIT==DENSE_USER&&sizeof(CSlot)==(1u<<vcl2::CSLOT_SHIFT));
// >>>> cl8g.h (pasted verbatim from /home/claude/vert/cl8g.h; vert/run_cl8g_tests.sh checks that the two texts are equal)
// ---- cl8g: eight click lines per call with OPAQUE click_id / user_id (GFAST.md package C; agent CL, 2026-11-21).
// ---- /home/claude/vert/cl8g.h; tested as this exact text by cl8g_test.cpp (run_cl8g_tests.sh).
// ---- Paste / include AFTER cl8_paste2.h (namespace vcl2 in main.cpp): it calls the proven version 2 pieces
// ---- (vcl2::tod8k_v5, day4k_v5, epoch8k_v5, off8k_v5, ld16x8_v5 and their scalar twins) and vcl2::EvDates
// ---- (make_dates_cl). AVX-512 F/BW/DQ/VL/CD, no gather instruction, zmm only. The hex pieces are P3's (ev8g.h).
// A line is cut in three 64-byte windows: A at the line start (click_id up to its comma), B behind that comma
// (user_id up to the second comma of the line), C behind the second comma (campaign_id, ts and the newline). So each
// part may be up to 63 bytes (a quoted 33-byte ID is 35; campaign + ts are at most ~40), a line up to 191; cl8
// version 2 wanted the whole line in 64. 224 readable bytes from every ls[i] (4096 zero bytes behind each file).
// An ok row is a line click_line_v3 / on_click would turn into a record once its campaign is found: four well-formed
// fields, click_id and user_id not empty, ts in one of the three fast forms and inside the click window. The caller
// looks the campaign of (sc, lc) up (camp_find): not found = the line is dropped. A drop row is a line the line
// code drops without a trace (an empty ID, or a ts the kernel decided that is outside the window). Any other row
// goes to the line code. The IDs are never decoded: class + exact key per ID.
namespace vclg{
using vcl2::EvDates;
static constexpr uint64_t CLS_SHORT=0,CLS_LONG=1,CLS_OTHER=2; // P1's GRec classes
struct ClgConst{int64_t clickStart;uint64_t wlen;EvDates d;}; // d = vcl2::make_dates_cl(clickStart, window_end, window_start)
struct alignas(64) Cl8gScan{uint64_t cA[8],qA[8],cB[8],qB[8],cC[8],qC[8],bB[8],bC[8];};
struct alignas(64) Cl8gOut{
 // all fields: ok rows only
 uint64_t kcls[8];    // click_id: 0 = 'k' + 0..8 bytes of [0-9a-z]; 1 = 'k' + exactly 32 of [0-9a-f]; 2 = any other text (not empty)
 uint64_t kkey[8];    // class 0: the bytes after the 'k' as a word (first byte lowest, zero padded); 1: the last 16 hex digits as a number; 2: pointer to the text (= ks)
 uint64_t khi[8];     // class 1: the first 16 hex digits as a number; else 0
 const char* ks[8];uint64_t kl[8]; // click_id text (quotes stripped) and its length (1..63)
 uint64_t ucls[8];    // user_id: 0 = at most 11 bytes (any bytes); 1 = 'u' + exactly 32 of [0-9a-f]; 2 = any other text
 uint64_t uk0[8],uk1[8]; // class 0: KeyTable::pack of the text (k0 = bytes 0..7, k1 = bytes 8..10 | (length + 1) << 24); else 0
 uint64_t ulo[8],uhi[8]; // class 1: as kkey / khi; else 0
 const char* us[8];uint64_t ul[8]; // user_id text (quotes stripped) and its length (1..63)
 uint64_t ckh[8];     // (ts - clickStart) << 16; the caller ORs (uint16_t)campaign number
 const char* sc[8];uint64_t lc[8]; // campaign_id content (quotes stripped)
 uint8_t ok;          // bit i: every field above is exact for row i
 uint8_t drop;        // bit i: the line code drops the line without doing anything; never together with ok
};
// ---------------- scalar twin: the same decisions, written row by row ----------------
static inline uint64_t hex16_ref(const char* s,bool& good){uint64_t v=0;for(int i=0;i<16;++i){const unsigned ch=(unsigned char)s[i];unsigned d;
  if(ch>='0'&&ch<='9')d=ch-'0';else if(ch>='a'&&ch<='f')d=ch-'a'+10;else{good=false;d=0;}v=(v<<4)|d;}return v;}
static inline void cl8g_ref(const char* const* ls,const char* const* nx,const ClgConst& c,Cl8gOut& o){
 using namespace vcl2;
 unsigned okm=0,drm=0;
 for(int i=0;i<8;++i){
  o.kcls[i]=o.kkey[i]=o.khi[i]=o.kl[i]=o.ucls[i]=o.uk0[i]=o.uk1[i]=o.ulo[i]=o.uhi[i]=o.ul[i]=o.ckh[i]=o.lc[i]=0;o.ks[i]=o.us[i]=o.sc[i]=ls[i];
  const char* const p=ls[i];const char* const nl=nx[i]-1;
  uint64_t cA,qA;masks_ref(p,cA,qA);
  if(!cA)continue;                                              // no comma in window A
  const unsigned c0=tz_ref(cA);const uint64_t mA=cA^(cA-1),enA=cA&~blsr_ref(cA);
  const uint64_t qa=qA&mA,sqA=qa&1,opA=(sqA+~enA)&enA,errA=opA^((qa&~1ull)<<1);
  const char* const bB=p+c0+1;
  uint64_t cB,qB;masks_ref(bB,cB,qB);
  if(!cB)continue;                                              // no comma in window B
  const unsigned e1=tz_ref(cB);const uint64_t mB=cB^(cB-1),enB=cB&~blsr_ref(cB);
  const uint64_t qb=qB&mB,sqB=qb&1,opB=(sqB+~enB)&enB,errB=opB^((qb&~1ull)<<1);
  const char* const bC=bB+e1+1;
  uint64_t cC,qC;masks_ref(bC,cC,qC);
  const uint64_t L=(uint64_t)(nl-bC);
  if(L>>6)continue;                                             // newline not inside window C (or before it)
  const uint64_t nbit=1ull<<L,keep=nbit-1;
  const uint64_t d=cC&keep,qc=qC&keep;
  const uint64_t st=(d<<1)|1,en=d|nbit;
  const uint64_t sq=qc&st,op=(sq+~en)&en,err=op^((qc&~st)<<1);
  if((errA|errB|err|(uint64_t)(__builtin_popcountll(d)^1))!=0)continue; // not four well-formed fields: the line code passes it on
  const unsigned z0=(unsigned)(qA&1),l0=c0-2*z0;
  const unsigned z1=(unsigned)(qB&1),l1=e1-2*z1;
  const unsigned d0=tz_ref(d),z2=(unsigned)(qc&1),l2=d0-2*z2;
  const unsigned z3=(unsigned)((qc>>(d0+1))&1),o3=d0+1+z3,l3=(unsigned)L-o3-z3;
  const char* const s0=p+z0;const char* const s1=bB+z1;const char* const s3=bC+o3;
  if(l0==0||l1==0){drm|=1u<<i;continue;}                        // dropped before anything is decoded
  // ts: the three fast forms of ts_v3; anything else is left to the line code
  int64_t ts;
  if(l3==13){uint64_t v;if(!epoch_ref(ld8_ref(s3),ld8_ref(s3+5),v))continue;ts=(int64_t)v;}
  else if(l3==20||l3==25){
   const uint64_t t0=ld8_ref(s3),t1=ld8_ref(s3+8),t2=ld8_ref(s3+12);
   uint64_t tods,days;
   if(!tod_ref(t1,t2,tods)||!day4_ref(t0,t1,c.d,days))continue;
   ts=(int64_t)(days+tods);
   if(l3==20){if(byte_of(t2,7)!='Z')continue;}
   else{int64_t delta;if(!off_ref(t2,ld8_ref(s3+17),delta))continue;ts+=delta;}
  }else continue;
  const uint64_t trel=(uint64_t)ts-(uint64_t)c.clickStart;
  if(trel>=c.wlen){drm|=1u<<i;continue;}                        // outside the click window: dropped
  okm|=1u<<i;
  o.ckh[i]=trel<<16;o.sc[i]=bC+z2;o.lc[i]=l2;
  // the two IDs, by a plain reading of the text
  o.ks[i]=s0;o.kl[i]=l0;o.us[i]=s1;o.ul[i]=l1;
  {uint64_t cls=CLS_OTHER,key=(uint64_t)s0,hi=0;
   if(s0[0]=='k'&&l0<=9){bool g=true;uint64_t w=0;for(unsigned j=1;j<l0;++j){const unsigned ch=(unsigned char)s0[j];if(!((ch>='0'&&ch<='9')||(ch>='a'&&ch<='z')))g=false;w|=(uint64_t)ch<<(8*(j-1));}
    if(g){cls=CLS_SHORT;key=w;}}
   else if(s0[0]=='k'&&l0==33){bool g=true;const uint64_t h=hex16_ref(s0+1,g),lo=hex16_ref(s0+17,g);if(g){cls=CLS_LONG;key=lo;hi=h;}}
   o.kcls[i]=cls;o.kkey[i]=key;o.khi[i]=hi;}
  {uint64_t cls=CLS_OTHER,k0=0,k1=0,lo=0,hi=0;
   if(l1<=11){cls=CLS_SHORT;for(unsigned j=0;j<l1&&j<8;++j)k0|=(uint64_t)(unsigned char)s1[j]<<(8*j);for(unsigned j=8;j<l1;++j)k1|=(uint64_t)(unsigned char)s1[j]<<(8*(j-8));k1|=(uint64_t)(l1+1)<<24;}
   else if(s1[0]=='u'&&l1==33){bool g=true;const uint64_t h=hex16_ref(s1+1,g),l=hex16_ref(s1+17,g);if(g){cls=CLS_LONG;lo=l;hi=h;}}
   o.ucls[i]=cls;o.uk0[i]=k0;o.uk1[i]=k1;o.ulo[i]=lo;o.uhi[i]=hi;}
 }
 o.ok=(uint8_t)okm;o.drop=(uint8_t)drm;
}
// ---------------- the scan: three windows per row, and where the next line starts ----------------
// s: the comma and quote masks of the three windows and the window starts bB (one past the first comma of A; p + 65
// if A has none) and bC (one past the first comma of B; bB + 65 if B has none). nx[i] = one past the first '\n' of
// window C. Result: bit i set = nx[i] is NOT certain to be the line's end (a '\n' at or before the comma that ends
// window A's or window B's part, one of those commas missing, or no '\n' in window C): the caller finds that row's
// newline itself (memchr from ls[i]). A clear bit: nx[i] - 1 is the first '\n' at or after ls[i].
#define VCLG_ROW(i) {const char* const p=ls[i]; \
  const __m512i x=_mm512_loadu_si512((const void*)p); \
  const uint64_t ca=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc)),na=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vn)); \
  s.cA[i]=ca;s.qA[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq)); \
  const uint64_t e0=_tzcnt_u64(ca);const char* const bb=p+e0+1; \
  const __m512i y=_mm512_loadu_si512((const void*)bb); \
  const uint64_t cb=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vc)),nb=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vn)); \
  s.cB[i]=cb;s.qB[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(y,vq));s.bB[i]=(uint64_t)bb; \
  const uint64_t e1=_tzcnt_u64(cb);const char* const bc=bb+e1+1; \
  const __m512i z=_mm512_loadu_si512((const void*)bc); \
  s.cC[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(z,vc));s.qC[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(z,vq));s.bC[i]=(uint64_t)bc; \
  const uint64_t nc=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(z,vn)),e3=_tzcnt_u64(nc); \
  nx[i]=bc+e3+1; \
  const uint64_t bad_=(na&(ca^(ca-1)))|(nb&(cb^(cb-1)))|((e0|e1|e3)>>6); /* (ca == 0: ca ^ (ca - 1) is all ones, and e0 is 64 anyway) */ \
  unc|=(unsigned)(bad_!=0)<<i;__asm__("":"+r"(unc));}
VERT_T5 static inline unsigned cscang8_v5(const char* const* ls,const char** nx,Cl8gScan& s){
 const __m512i vc=_mm512_set1_epi8(','),vq=_mm512_set1_epi8('"'),vn=_mm512_set1_epi8('\n');
 unsigned unc=0;
 VCLG_ROW(0) VCLG_ROW(1) VCLG_ROW(2) VCLG_ROW(3) VCLG_ROW(4) VCLG_ROW(5) VCLG_ROW(6) VCLG_ROW(7)
 return unc;
}
#undef VCLG_ROW
// ---------------- vector pieces for the IDs (the text of vertg::hex8k_v5 / hex32k_v5, ev8g.h) ----------------
// Eight ASCII hex digits per lane (first digit in the lowest byte) -> their value; bad collects a non-zero byte for
// every character that is not 0-9 a-f.
VERT_T5 static inline __attribute__((always_inline)) __m512i hex8k_v5(__m512i w,__m512i& bad){
 const __m512i d=_mm512_sub_epi8(w,_mm512_set1_epi8('0')),a=_mm512_sub_epi8(w,_mm512_set1_epi8('a'));
 bad=_mm512_or_si512(bad,_mm512_min_epu8(_mm512_subs_epu8(d,_mm512_set1_epi8(9)),_mm512_subs_epu8(a,_mm512_set1_epi8(5))));
 const __m512i nib=_mm512_mask_add_epi8(d,_mm512_cmpgt_epi8_mask(w,_mm512_set1_epi8(0x40)),a,_mm512_set1_epi8(10));
 const __m512i by=_mm512_maddubs_epi16(nib,_mm512_set1_epi16(0x0110));          // two digits -> one byte value per 16-bit word
 const __m512i dw=_mm512_madd_epi16(by,_mm512_set1_epi32(0x00010100));           // two of those -> 16 bits per 32-bit word
 return _mm512_or_si512(_mm512_slli_epi64(_mm512_and_si512(dw,_mm512_set1_epi64(0xFFFF)),16),_mm512_srli_epi64(dw,32));
}
// 32 hex digits in W0..W3 (text bytes 1..8, 9..16, 17..24, 25..32) -> hi / lo; good = every character is a hex digit.
VERT_T5 static inline __attribute__((always_inline)) __mmask8 hex32k_v5(__m512i W0,__m512i W1,__m512i W2,__m512i W3,__m512i& hi,__m512i& lo){
 __m512i bad=_mm512_setzero_si512();
 const __m512i v0=hex8k_v5(W0,bad),v1=hex8k_v5(W1,bad),v2=hex8k_v5(W2,bad),v3=hex8k_v5(W3,bad);
 hi=_mm512_or_si512(_mm512_slli_epi64(v0,32),v1);lo=_mm512_or_si512(_mm512_slli_epi64(v2,32),v3);
 return _mm512_testn_epi64_mask(bad,bad);
}
// ---------------- the decode ----------------
VERT_T5 static inline void cdecg8_v5(const char* const* ls,const char* const* nx,const Cl8gScan& s,const ClgConst& c,Cl8gOut& o){
 using namespace vcl2;
 #define VG_C(x) _mm512_set1_epi64((long long)(x))
 #define VG_TZ(x,xm) _mm512_sub_epi64(c63,_mm512_lzcnt_epi64(_mm512_xor_si512(x,xm))) /* tzcnt(x) given xm = x-1 (63 for x == 0) */
 #define VG_ERR(q,st,en,sq,op) const __m512i sq=_mm512_and_si512(q,st),op=_mm512_and_si512(_mm512_add_epi64(_mm512_sub_epi64(sq,en),ones),en)
 const __m512i one=VG_C(1),ones=VG_C(-1),c63=VG_C(63),c64=VG_C(64);
 const __m512i P=_mm512_loadu_si512((const void*)ls),NL=_mm512_add_epi64(_mm512_loadu_si512((const void*)nx),ones);
 const __m512i BB=_mm512_load_si512((const void*)s.bB),BC=_mm512_load_si512((const void*)s.bC);
 const __m512i CA=_mm512_load_si512((const void*)s.cA),QA=_mm512_load_si512((const void*)s.qA),CB=_mm512_load_si512((const void*)s.cB),QB=_mm512_load_si512((const void*)s.qB);
 const __m512i CC=_mm512_load_si512((const void*)s.cC),QC=_mm512_load_si512((const void*)s.qC);
 // window A: the click_id
 const __m512i tA=_mm512_add_epi64(CA,ones),c0=_mm512_sub_epi64(_mm512_sub_epi64(BB,P),one),mA=_mm512_xor_si512(CA,tA),enA=_mm512_andnot_si512(_mm512_and_si512(CA,tA),CA);
 const __m512i qa=_mm512_and_si512(QA,mA);
 VG_ERR(qa,one,enA,sqA,opA);
 const __m512i errA=_mm512_xor_si512(opA,_mm512_slli_epi64(_mm512_andnot_si512(one,qa),1));
 // window B: the user_id
 const __m512i tB=_mm512_add_epi64(CB,ones),e1=_mm512_sub_epi64(_mm512_sub_epi64(BC,BB),one),mB=_mm512_xor_si512(CB,tB),enB=_mm512_andnot_si512(_mm512_and_si512(CB,tB),CB);
 const __m512i qb=_mm512_and_si512(QB,mB);
 VG_ERR(qb,one,enB,sqB,opB);
 const __m512i errB=_mm512_xor_si512(opB,_mm512_slli_epi64(_mm512_andnot_si512(one,qb),1));
 // window C: campaign_id, ts, newline
 const __m512i L=_mm512_sub_epi64(NL,BC),nbit=_mm512_sllv_epi64(one,_mm512_and_si512(L,c63)),keep=_mm512_add_epi64(nbit,ones);
 const __m512i d=_mm512_and_si512(CC,keep),qc=_mm512_and_si512(QC,keep);
 const __m512i st=_mm512_or_si512(_mm512_slli_epi64(d,1),one),en=_mm512_or_si512(d,nbit);
 VG_ERR(qc,st,en,sq,op);
 const __m512i errC=_mm512_xor_si512(op,_mm512_slli_epi64(_mm512_andnot_si512(st,qc),1));
 const __m512i u0=_mm512_add_epi64(d,ones),d0=VG_TZ(d,u0),dR=_mm512_and_si512(d,u0);
 const __m512i err=_mm512_or_si512(_mm512_ternarylogic_epi64(errA,errB,errC,0xFE),dR);  // a quote error, or a second comma before the newline
 __mmask8 sok=_mm512_testn_epi64_mask(err,err);
 sok=_mm512_mask_test_epi64_mask(sok,d,d);                                       // a comma in window C before the newline
 sok=_mm512_mask_test_epi64_mask(sok,CA,CA);                                     // a comma in window A
 sok=_mm512_mask_test_epi64_mask(sok,CB,CB);                                     // a comma in window B
 sok=_mm512_mask_cmplt_epu64_mask(sok,L,c64);                                    // the newline inside window C
 const __m512i z0=_mm512_and_si512(QA,one),l0=_mm512_sub_epi64(c0,_mm512_add_epi64(z0,z0));
 const __m512i z1=_mm512_and_si512(QB,one),l1=_mm512_sub_epi64(e1,_mm512_add_epi64(z1,z1));
 const __m512i z2=_mm512_and_si512(qc,one),l2=_mm512_sub_epi64(d0,_mm512_add_epi64(z2,z2));
 const __m512i a3=_mm512_add_epi64(d0,one),z3=_mm512_and_si512(_mm512_srlv_epi64(qc,a3),one),o3=_mm512_add_epi64(a3,z3),l3=_mm512_sub_epi64(_mm512_sub_epi64(L,o3),z3);
 const __mmask8 nz=_mm512_mask_test_epi64_mask(_mm512_mask_test_epi64_mask(sok,l0,l0),l1,l1); // well-formed, click_id and user_id not empty
 const __mmask8 dropE=_kandn_mask8(nz,sok);                                      // well-formed with an empty ID
 // ---- operands: the field addresses go through memory into general registers, 16-byte loads, no gather ----
 // Every address is at most line start + 65 + 65 + 66 (bB, bC and o3 are bounded whatever the masks hold), so every
 // load stays inside the 224 readable bytes.
 const __m512i S0=_mm512_add_epi64(P,z0),S1=_mm512_add_epi64(BB,z1),S2=_mm512_add_epi64(BC,z2),S3=_mm512_add_epi64(BC,o3);
 alignas(64) uint64_t AD[24];
 _mm512_store_si512((void*)AD,S0);_mm512_store_si512((void*)(AD+8),S1);_mm512_store_si512((void*)(AD+16),S3);
 const uint64_t* ad=AD;__asm__("":"+r"(ad)::"memory");
 __m512i K0,K1,K2,K3,K4,Kx,U0,U1,U2,U3,U4,Ux,ts0,tsh,ts2,tsx;
 ld16x8_v5(ad,0,K0,K1);          // click_id bytes 0..15
 ld16x8_v5(ad,9,K2,K3);          // 9..24
 ld16x8_v5(ad,25,K4,Kx);         // 25..40
 ld16x8_v5(ad+8,0,U0,U1);        // user_id bytes 0..15
 ld16x8_v5(ad+8,9,U2,U3);
 ld16x8_v5(ad+8,25,U4,Ux);
 ld16x8_v5(ad+16,0,ts0,tsh);     // ts bytes 0..15
 ld16x8_v5(ad+16,12,ts2,tsx);    // ts bytes 12..27
 (void)Kx;(void)Ux;
 const __m512i lowbyte=VG_C(0xFF),c8=VG_C(8),c33=VG_C(33);
 // ---- click_id: class, key ----
 __m512i kcls,kkey,khi;
 {const __m512i W=_mm512_or_si512(_mm512_srli_epi64(K0,8),_mm512_slli_epi64(K1,56));       // text bytes 1..8
  const __mmask8 let=_mm512_cmpeq_epi64_mask(_mm512_and_si512(K0,lowbyte),VG_C('k'));
  const __m512i n=_mm512_sub_epi64(l0,one);                                                 // bytes after the letter (l0 >= 1 in an ok row)
  const __m512i keepb=_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(n,3))); // n == 0 or n > 8: the shift count is 64 or more and gives 0 (n > 8 is not short anyway)
  const __m512i w=_mm512_and_si512(W,keepb);
  const __m512i x=_mm512_or_si512(w,_mm512_andnot_si512(keepb,_mm512_set1_epi8('0')));
  const __m512i dd=_mm512_sub_epi8(x,_mm512_set1_epi8('0')),aa=_mm512_sub_epi8(x,_mm512_set1_epi8('a'));
  const __m512i bad=_mm512_min_epu8(_mm512_subs_epu8(dd,_mm512_set1_epi8(9)),_mm512_subs_epu8(aa,_mm512_set1_epi8(25)));
  const __mmask8 sh=_mm512_mask_testn_epi64_mask(_mm512_mask_cmple_epu64_mask(let,n,c8),bad,bad);
  __m512i hi,lo;const __mmask8 lg=_kand_mask8(_mm512_mask_cmpeq_epi64_mask(let,l0,c33),hex32k_v5(W,K2,K3,K4,hi,lo));
  kcls=_mm512_mask_mov_epi64(_mm512_mask_mov_epi64(VG_C(CLS_OTHER),lg,one),sh,_mm512_setzero_si512());
  kkey=_mm512_mask_mov_epi64(_mm512_mask_mov_epi64(S0,lg,lo),sh,w);
  khi=_mm512_maskz_mov_epi64(lg,hi);}
 // ---- user_id: class, KeyTable words, long value ----
 __m512i ucls,uk0,uk1,ulo,uhi;
 {const __m512i W=_mm512_or_si512(_mm512_srli_epi64(U0,8),_mm512_slli_epi64(U1,56));
  const __mmask8 let=_mm512_cmpeq_epi64_mask(_mm512_and_si512(U0,lowbyte),VG_C('u'));
  const __mmask8 pk=_mm512_cmple_epu64_mask(l1,VG_C(11));
  const __m512i n0=_mm512_min_epu64(l1,c8),n1=_mm512_sub_epi64(l1,n0);                      // text bytes in k0 / in k1
  const __m512i k0=_mm512_and_si512(U0,_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(n0,3))));
  const __m512i k1=_mm512_or_si512(_mm512_and_si512(U1,_mm512_srlv_epi64(ones,_mm512_sub_epi64(c64,_mm512_slli_epi64(n1,3)))),_mm512_slli_epi64(_mm512_add_epi64(l1,one),24));
  __m512i hi,lo;const __mmask8 lg=_kand_mask8(_mm512_mask_cmpeq_epi64_mask(let,l1,c33),hex32k_v5(W,U2,U3,U4,hi,lo));
  ucls=_mm512_mask_mov_epi64(_mm512_mask_mov_epi64(VG_C(CLS_OTHER),lg,one),pk,_mm512_setzero_si512());
  uk0=_mm512_maskz_mov_epi64(pk,k0);uk1=_mm512_maskz_mov_epi64(pk,k1);ulo=_mm512_maskz_mov_epi64(lg,lo);uhi=_mm512_maskz_mov_epi64(lg,hi);}
 // ---- ts: 20 (Z), 13 (digits), 25 (offset): the text of cl8 version 2 ----
 const __m512i cZ=VG_C('Z');
 const __mmask8 is13=_mm512_cmpeq_epi64_mask(l3,VG_C(13));
 const __m512i ts1=_mm512_mask_or_epi64(tsh,is13,_mm512_srli_epi64(ts0,40),_mm512_slli_epi64(tsh,24)); // bytes 8..15, for 13 digits bytes 5..12
 const __m512i ts3=_mm512_or_si512(_mm512_srli_epi64(ts2,40),_mm512_slli_epi64(tsx,24));               // bytes 17..24
 __mmask8 oks,hits,okoff,okep;
 const __m512i days=day4k_v5(ts0,ts1,c.d,hits);
 const __m512i tods=tod8k_v5(ts1,ts2,oks);
 const __mmask8 dt=_kand_mask8(oks,hits);                                        // valid time of day and known date
 const __m512i ep=epoch8k_v5(ts0,ts1,okep);
 const __m512i base=_mm512_mask_mov_epi64(_mm512_add_epi64(days,tods),is13,ep);
 const __mmask8 t13=_kand_mask8(is13,okep);
 const __m512i b19=_mm512_srli_epi64(ts2,56);
 const __mmask8 tokA=_kor_mask8(t13,_mm512_mask_cmpeq_epi64_mask(_mm512_mask_cmpeq_epi64_mask(dt,l3,VG_C(20)),b19,cZ)); // 13 digits, or 20 bytes ending in 'Z'
 const __m512i off=off8k_v5(ts3,okoff);
 const __mmask8 o25=_kand_mask8(dt,_mm512_mask_cmpeq_epi64_mask(okoff,l3,VG_C(25)));                                    // 25 bytes with a valid offset
 const __mmask8 p25=_mm512_mask_cmpeq_epi64_mask(o25,b19,VG_C('+'));
 __m512i ts=_mm512_mask_sub_epi64(base,p25,base,off);
 const __mmask8 m25=_mm512_mask_cmpeq_epi64_mask(o25,b19,VG_C('-'));
 ts=_mm512_mask_add_epi64(ts,m25,ts,off);
 // ts decided by the kernel: 13 digits, or (valid time and known date) and (Z form, or offset form with a valid offset)
 const __mmask8 tsk=_kand_mask8(nz,_kor_mask8(tokA,_kor_mask8(p25,m25)));
 const __m512i trel=_mm512_sub_epi64(ts,VG_C(c.clickStart));
 const __mmask8 ok=_mm512_mask_cmplt_epu64_mask(tsk,trel,VG_C(c.wlen));
 const __mmask8 drop=_kor_mask8(dropE,_kandn_mask8(ok,tsk));                     // an empty ID, or ts outside the window
 _mm512_store_si512((void*)o.kcls,kcls);_mm512_store_si512((void*)o.kkey,kkey);_mm512_store_si512((void*)o.khi,khi);
 _mm512_store_si512((void*)o.ks,S0);_mm512_store_si512((void*)o.kl,l0);
 _mm512_store_si512((void*)o.ucls,ucls);_mm512_store_si512((void*)o.uk0,uk0);_mm512_store_si512((void*)o.uk1,uk1);
 _mm512_store_si512((void*)o.ulo,ulo);_mm512_store_si512((void*)o.uhi,uhi);
 _mm512_store_si512((void*)o.us,S1);_mm512_store_si512((void*)o.ul,l1);
 _mm512_store_si512((void*)o.ckh,_mm512_slli_epi64(trel,16));
 _mm512_store_si512((void*)o.sc,S2);_mm512_store_si512((void*)o.lc,l2);
 o.ok=(uint8_t)_cvtmask8_u32(ok);o.drop=(uint8_t)_cvtmask8_u32(drop);
 #undef VG_C
 #undef VG_TZ
 #undef VG_ERR
}
// One call for a group whose line ends are known (tests; the lane loop calls cscang8_v5 one group ahead).
VERT_T5 static inline void cl8g_v5(const char* const* ls,const char* const* nx,const ClgConst& c,Cl8gOut& o){
 Cl8gScan s;const char* nn[8];(void)cscang8_v5(ls,nn,s);cdecg8_v5(ls,nx,s,c,o);
}
} // namespace vclg
// <<<< cl8g.h
// ---- lanes, clicks: the scan stage of cl8 (versions 1 and 2 share it) that also yields where the next line of each
// ---- row starts. /home/claude/vert/lane_cl_paste.h; tested as this exact text by lanes/lane_test.cpp
// ---- (Cl8Scan == vcl2::cscan8_v5 byte for byte; nx == memchr). Paste after cl8_paste2.h. Decode: unchanged vcl2::cdec8_v5.
namespace vlane{
// s: exactly what vcl2::cscan8_v5(ls, s) stores. Result 0: every nx[i] is one past the first '\n' at or after ls[i].
// Result not 0 (0xFF; some row has no '\n' in the 64 bytes at ls[i]): no nx[i] may be used, the caller finds the
// eight newlines itself. (acc: OR of the eight tzcnt results, 64 = no newline.)
#define VLANE_CL_ROW(i) {const char* const p=ls[i]; \
  const __m512i x=_mm512_loadu_si512((const void*)p); \
  s.c[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vc));s.q[i]=_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vq)); \
  const uint64_t e=_tzcnt_u64(_cvtmask64_u64(_mm512_cmpeq_epi8_mask(x,vn))); \
  nx[i]=p+e+1;acc|=e;__asm__("":"+r"(acc));}
VERT_T5 static inline unsigned clscan8_v5(const char* const* ls,const char** nx,vcl2::Cl8Scan& s){
 const __m512i vc=_mm512_set1_epi8(','),vq=_mm512_set1_epi8('"'),vn=_mm512_set1_epi8('\n');
 uint64_t acc=0;
 VLANE_CL_ROW(0) VLANE_CL_ROW(1) VLANE_CL_ROW(2) VLANE_CL_ROW(3) VLANE_CL_ROW(4) VLANE_CL_ROW(5) VLANE_CL_ROW(6) VLANE_CL_ROW(7)
 return (acc>>6)!=0?0xFFu:0u;
}
#undef VLANE_CL_ROW
} // namespace vlane
static inline void cl8_scan_none(const char* const*,vcl::Cl8Scan&){}
[[gnu::noinline]] static void cl8_dec_ref(const char* const* ls,const char* const* nx,const vcl::Cl8Scan&,const vcl::ClConst& c,vcl::Cl8Out& o){vcl::cl8_ref(ls,nx,c,o);}
static inline void cl8_scan_none2(const char* const*,vcl2::Cl8Scan&){}
[[gnu::noinline]] static void cl8_dec_ref2(const char* const* ls,const char* const* nx,const vcl2::Cl8Scan&,const vcl2::ClConst& c,vcl2::Cl8Out& o){vcl2::cl8_ref(ls,nx,c,o);}
// The constants of a loop call for either kernel version. Version 2 gets the months of the run's own click window:
// without `d` it would leave every row with a date-form ts to the line code.
#define SS_CL8_CONST1(lp_) vcl::ClConst{lp_.kc,lp_.limC,lp_.ku,dense.limI,denseComplex,lp_.clickStart,lp_.wlen,(const vcl::DateEnt*)lp_.memo}
static inline vcl2::ClConst cl8_const2(uint64_t kc,uint64_t limC,uint64_t ku,uint64_t limI,const uint64_t* cplx,int64_t clickStart,uint64_t wlen){
 vcl2::ClConst cc{kc,limC,ku,limI,cplx,clickStart,wlen,nullptr};cc.d=vcl2::make_dates_cl(clickStart,window_end,window_start);return cc;}
#define SS_CL8_CONST2(lp_) cl8_const2(lp_.kc,lp_.limC,lp_.ku,dense.limI,denseComplex,lp_.clickStart,lp_.wlen)
#define SS_CL8_SLOT(O_,i_) (*(CSlot*)((char*)lp.cl+O_.so[i_]))
// One ok row: the row code's campaign lookup; a campaign that is not found drops the row without a trace.
#define SS_CL8_ROW(O_,i_) {uint32_t ci_; \
   if(__builtin_expect(camp_find(lp.cf,lp.camps,O_.sc[i_],(size_t)O_.lc[i_],ci_),1)){found_|=1u<<(i_);cslot_commit(SS_CL8_SLOT(O_,i_),O_.ckh[i_]|(uint16_t)ci_,(uint32_t)O_.user[i_],lp.tid);}}
// Finishing the first N rows of a group in line order. The maxima of the group (umax, kend) cover all its ok rows:
// they are used only if every ok row was committed, otherwise the committed rows are taken one by one.
#define SS_CL8_FINISH(O,LS,NX,N,MASKS) {const auto& O_=(O);const char* const* const L_=(LS);const char* const* const X_=(NX);const unsigned n_=(N); \
  const unsigned ok_=O_.ok&((1u<<n_)-1);unsigned found_=0; \
  if(__builtin_expect(ok_==0xFFu,1)){SS_CL8_ROW(O_,0) SS_CL8_ROW(O_,1) SS_CL8_ROW(O_,2) SS_CL8_ROW(O_,3) SS_CL8_ROW(O_,4) SS_CL8_ROW(O_,5) SS_CL8_ROW(O_,6) SS_CL8_ROW(O_,7)} \
  else for(unsigned i_=0;i_<n_;++i_){ \
   if((ok_>>i_)&1)SS_CL8_ROW(O_,i_) \
   else if(!((O_.drop>>i_)&1)){const char* const q_=L_[i_];++slow;click_line_v3<true>(q_,MASKS(q_),X_[i_]-1,lp,pend,pp,dmax,send,ring,rpos,general);}} \
  if(__builtin_expect(found_==ok_,1)){dmax=max(dmax,O_.umax);send=max(send,O_.kend);} \
  else for(unsigned x_=found_;x_;x_&=x_-1){const unsigned i_=(unsigned)__builtin_ctz(x_);dmax=max(dmax,(uint32_t)O_.user[i_]);send=max(send,(uint32_t)O_.kcn[i_]+1);} }
#if defined(SS_ABL_CLICK)&&SS_ABL_CLICK>=2
#define SS_CL8_PF(O_,i_) ;
#else
#define SS_CL8_PF(O_,i_) __builtin_prefetch(&SS_CL8_SLOT(O_,i_),1,3);
#endif
#define SS_CL8_PFW(O_,i_) SS_PFW_ASM(&SS_CL8_SLOT(O_,i_))
// The line index and the three groups under way (N scanned, D decoded and its slots asked for, F finished) are those
// of SS_EVENT_LOOP_EV8; see there. A short last group is filled up with the pointer behind its last line (a row with
// nx == ls is never ok, drop or idok; the row masks cut it off as well).
#define SS_CLICK_LOOP_CL8(NAME,TARGET,MASKS,NLMASK,SCAN,DEC,VNS,CCONST) \
template<unsigned LEAD,class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& camps,size_t PC,int64_t clickStart,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 const char* const end=base+len;const unsigned pfw_=optPfw; \
 ClLoop lp_(tl,tab,camps,PC,end,clickStart);lp_.ahead=0;const ClLoop& lp=lp_; /* no ring: a slow row writes its slot at the end of its line, a group after the kernel asked for it */ \
 PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;CRing* const ring=nullptr;unsigned rpos=0;uint64_t rows=0,slow=0; \
 const char* ls[EV8_BLOCK+64+64];size_t pos=0,m=0;ls[0]=p;const char* scan=p; \
 const VNS::ClConst cc=CCONST(lp); \
 VNS::Cl8Out out[4];VNS::Cl8Scan sc[2];const char* tls[9]; \
 const char* const* gl[4]={ls,ls,ls,ls};unsigned gn[4]={0,0,0,0}; /* group g lives in slot g&3: its lines (the next line of row i is [i+1]), its number of rows, its results */ \
 unsigned t_=0,live=0;bool more=true; /* turn t_: group t_ is formed and scanned (N, sc[t_&1]), group t_-1 decoded and its slots asked for (D), group t_-1-LEAD finished (F); live = groups formed and not finished */ \
 while(more||live){ \
  const unsigned iN=t_&3,iD=(t_-1)&3,iF=(t_-1-LEAD)&3,sN=t_&1; \
  unsigned n=0; \
  if(more){ \
   if(pos+8>m){ /* fewer than eight whole lines known: what is still needed moves to the front and the index goes on */ \
    size_t s_=pos;for(unsigned j_=1;j_<=LEAD+1;++j_)s_-=gn[(t_-j_)&3]?8:0; /* the groups under way keep their eight starts each right before ls[pos] */ \
    if(s_){for(size_t i_=0;i_<=m-s_;i_+=4)_mm256_storeu_si256((__m256i*)(ls+i_),_mm256_loadu_si256((const __m256i*)(ls+s_+i_)));pos-=s_;m-=s_; \
     for(unsigned j_=1;j_<=LEAD+1;++j_)if(gn[(t_-j_)&3])gl[(t_-j_)&3]-=s_;} \
    while(m-pos<8&&scan<end&&ls[m]<lim){ \
     const char* const stop=(size_t)(end-scan)>EV8_BLOCK?scan+EV8_BLOCK:end; \
     const char** w_=ls+m; /* the first two newlines of a window are stored without a branch (click lines are shorter than a window: one or two per window; a missing one stores a value past the last entry, never read); a third one is rare */ \
     do{uint64_t x_=NLMASK(scan);const uint64_t y_=_blsr_u64(x_);w_[1]=scan+_tzcnt_u64(x_)+1;w_[2]=scan+_tzcnt_u64(y_)+1;w_+=(x_!=0)+(y_!=0);x_=_blsr_u64(y_); \
      while(__builtin_expect(x_!=0,0)){*++w_=scan+_tzcnt_u64(x_)+1;x_=_blsr_u64(x_);} \
      scan+=64;}while(scan<stop); \
     m=(size_t)(w_-ls);} \
   } \
   if(__builtin_expect(pos+8<=m&&ls[pos+7]<lim,1)){n=8;gl[iN]=ls+pos;} \
   else{ /* the last lines of the chunk: a short group */ \
    more=false; \
    while(pos+n<m&&ls[pos+n]<lim)++n; \
    if(n){for(unsigned i_=0;i_<9;++i_)tls[i_]=ls[pos+(i_<n?i_:n)];gl[iN]=tls;} \
   } \
   pos+=n;rows+=n; \
   if(n)SCAN(gl[iN],sc[sN]); \
  } \
  gn[iN]=n;live+=(n!=0); \
  if(gn[iD]){ \
   VNS::Cl8Out& o_=out[iD];const unsigned nd_=gn[iD]; \
   DEC(gl[iD],gl[iD]+1,sc[sN^1],cc,o_); \
   if(__builtin_expect(pfw_,0)){for(unsigned x_=o_.idok&((1u<<nd_)-1);x_;x_&=x_-1)SS_CL8_PFW(o_,__builtin_ctz(x_))} \
   else if(__builtin_expect((nd_==8)&(o_.idok==0xFFu),1)){SS_CL8_PF(o_,0) SS_CL8_PF(o_,1) SS_CL8_PF(o_,2) SS_CL8_PF(o_,3) SS_CL8_PF(o_,4) SS_CL8_PF(o_,5) SS_CL8_PF(o_,6) SS_CL8_PF(o_,7)} \
   else for(unsigned x_=o_.idok&((1u<<nd_)-1);x_;x_&=x_-1)SS_CL8_PF(o_,__builtin_ctz(x_)) \
  } \
  if(gn[iF]){SS_CL8_FINISH(out[iF],gl[iF],gl[iF]+1,gn[iF],MASKS) --live;} \
  ++t_; \
 } \
 if(pos==m&&ls[m]<lim)line_general<4>(ls[m],end,general); /* the last line has no newline */ \
 tl.denseMax=max(tl.denseMax,dmax);tl.clEnd=max(tl.clEnd,send);tl.cl8Rows+=rows;tl.cl8Slow+=slow; \
}
// _c8 = kernel version 2 (default), _c8v1 = version 1 (SS_CL8=1, for comparing the two from one binary).
SS_CLICK_LOOP_CL8(clicks_avx2_c8,SS_V3,win_masks_avx2,nl_mask_avx2,cl8_scan_none2,cl8_dec_ref2,vcl2,SS_CL8_CONST2)
SS_CLICK_LOOP_CL8(clicks_avx512z_c8,SS_V5,win_masks_avx512z,nl_mask_avx512z,vcl2::cscan8_v5,vcl2::cdec8_v5,vcl2,SS_CL8_CONST2)
SS_CLICK_LOOP_CL8(clicks_avx512y_c8,SS_V5,win_masks_avx512y,nl_mask_avx512y,vcl2::cscan8_v5,vcl2::cdec8_v5,vcl2,SS_CL8_CONST2)
SS_CLICK_LOOP_CL8(clicks_avx2_c8v1,SS_V3,win_masks_avx2,nl_mask_avx2,cl8_scan_none,cl8_dec_ref,vcl,SS_CL8_CONST1)
SS_CLICK_LOOP_CL8(clicks_avx512z_c8v1,SS_V5,win_masks_avx512z,nl_mask_avx512z,vcl::cscan8_v5,vcl::cdec8_v5,vcl,SS_CL8_CONST1)
SS_CLICK_LOOP_CL8(clicks_avx512y_c8v1,SS_V5,win_masks_avx512y,nl_mask_avx512y,vcl::cscan8_v5,vcl::cdec8_v5,vcl,SS_CL8_CONST1)
// ---- lanes: the cl8 click loop without a line index (see "lanes" at the ev8 loop; SS_LANES=0 keeps the loop above) ----
// The lane scan is vlane::clscan8_v5 (the text of cscan8_v5 + one newline compare on the window it loads): a click
// line of up to 63 bytes has its newline there, a longer one goes through memchr (lane_fix). The group maxima are
// used only if every row the kernel calls ok is a row of this chunk and was committed.
SS_V3 [[gnu::noinline]] static unsigned lane_scan_cl_avx2(const char* const* ls,const char** nx,vcl2::Cl8Scan&){
 unsigned unc=0;
 for(unsigned i=0;i<8;++i){const char* const p=ls[i];const uint64_t n=nl_mask_avx2(p);nx[i]=p+_tzcnt_u64(n)+1;if(n==0)unc|=1u<<i;}
 return unc;
}
#define SS_CL8L_FINISH(O,LS,NX,M,MASKS) {const auto& O_=(O);const char* const* const L_=(LS);const char* const* const X_=(NX);const unsigned m_=(M); \
  const unsigned ok_=O_.ok&m_;unsigned found_=0; \
  if(__builtin_expect(ok_==0xFFu,1)){rows+=8;SS_CL8_ROW(O_,0) SS_CL8_ROW(O_,1) SS_CL8_ROW(O_,2) SS_CL8_ROW(O_,3) SS_CL8_ROW(O_,4) SS_CL8_ROW(O_,5) SS_CL8_ROW(O_,6) SS_CL8_ROW(O_,7)} \
  else{rows+=(unsigned)__builtin_popcount(m_); \
   for(unsigned r_=m_;r_;r_&=r_-1){const unsigned i_=(unsigned)__builtin_ctz(r_); \
    if((ok_>>i_)&1)SS_CL8_ROW(O_,i_) \
    else if(!((O_.drop>>i_)&1)){const char* const q_=L_[i_];++slow;click_line_v3<true>(q_,MASKS(q_),X_[i_]-1,lp,pend,pp,dmax,send,ring,rpos,general);}}} \
  if(__builtin_expect(found_==(unsigned)O_.ok,1)){dmax=max(dmax,O_.umax);send=max(send,O_.kend);} \
  else for(unsigned x_=found_;x_;x_&=x_-1){const unsigned i_=(unsigned)__builtin_ctz(x_);dmax=max(dmax,(uint32_t)O_.user[i_]);send=max(send,(uint32_t)O_.kcn[i_]+1);} }
#define SS_CLICK_LOOP_CL8L(NAME,TARGET,MASKS,LSCAN,LIVE,DEC,VNS,CCONST) \
template<class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& camps,size_t PC,int64_t clickStart,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 if(p>=lim)return; \
 const char* const end=base+len; \
 ClLoop lp_(tl,tab,camps,PC,end,clickStart);lp_.ahead=0;const ClLoop& lp=lp_; /* no ring: a slow row writes its slot at the end of its line, a group after the kernel asked for it */ \
 PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;CRing* const ring=nullptr;unsigned rpos=0;uint64_t rows=0,slow=0,shortG=0,fixed=0; \
 const VNS::ClConst cc=CCONST(lp); \
 VNS::Cl8Out out[2];VNS::Cl8Scan sc[2]; \
 alignas(64) const char* A[4][8];alignas(64) const char* le[8];const char* tail=nullptr; \
 {const char* cut_[9];lane_cut(p,lim,end,cut_);for(unsigned i_=0;i_<8;++i_){A[0][i_]=cut_[i_];le[i_]=cut_[i_+1];}} \
 const char* const* lD=A[0];const char* const* xD=A[0];const char* const* lF=A[0];const char* const* xF=A[0]; \
 unsigned g=0,nD=0,nF=0;VNS::Cl8Scan* sN=&sc[0];VNS::Cl8Scan* sD=&sc[1];VNS::Cl8Out* oD=&out[0];VNS::Cl8Out* oF=&out[1];bool more=true; \
 do{ \
  unsigned nN=0;const char** const lN=A[g&3];const char** const xN=A[(g+1)&3]; \
  if(more){ \
   const unsigned live_=LIVE(lN,le); \
   if(__builtin_expect(live_==0xFFu,1)){nN=0xFFu;const unsigned unc_=LSCAN(lN,xN,*sN);if(__builtin_expect(unc_!=0,0))nN&=~lane_fix(lN,xN,unc_,end,tail,fixed);SS_LANE_PF1(xN)} \
   else if(live_){nN=live_;++shortG;const unsigned unc_=LSCAN(lN,xN,*sN)&live_;if(unc_)nN&=~lane_fix(lN,xN,unc_,end,tail,fixed); \
    for(unsigned i_=0;i_<8;++i_)if(!((live_>>i_)&1))xN[i_]=lN[i_];} /* a lane that is done stays where it is */ \
   else more=false; \
  } \
  if(nD){ \
   VNS::Cl8Out& o_=*oD; \
   DEC(lD,xD,*sD,cc,o_); \
   if(__builtin_expect((nD==0xFFu)&(o_.idok==0xFFu),1)){SS_CL8_PF(o_,0) SS_CL8_PF(o_,1) SS_CL8_PF(o_,2) SS_CL8_PF(o_,3) SS_CL8_PF(o_,4) SS_CL8_PF(o_,5) SS_CL8_PF(o_,6) SS_CL8_PF(o_,7)} \
   else for(unsigned x_=o_.idok&nD;x_;x_&=x_-1)SS_CL8_PF(o_,__builtin_ctz(x_)) \
  } \
  if(nF)SS_CL8L_FINISH(*oF,lF,xF,nF,MASKS) \
  lF=lD;xF=xD;nF=nD;lD=lN;xD=xN;nD=nN;++g; \
  {VNS::Cl8Scan* const t_=sN;sN=sD;sD=t_;VNS::Cl8Out* const u_=oD;oD=oF;oF=u_;} \
 }while(more||nD||nF); \
 if(tail)line_general<4>(tail,end,general); /* the last line has no newline */ \
 tl.denseMax=max(tl.denseMax,dmax);tl.clEnd=max(tl.clEnd,send);tl.cl8Rows+=rows;tl.cl8Slow+=slow;tl.laneShort+=shortG;tl.laneFixC+=fixed; \
}
// _c8l = the lane form of _c8 (kernel version 2 only). The avx2 instance is the test stand-in (scalar kernel).
SS_CLICK_LOOP_CL8L(clicks_avx2_c8l,SS_V3,win_masks_avx2,lane_scan_cl_avx2,lane_live_scalar,cl8_dec_ref2,vcl2,SS_CL8_CONST2)
SS_CLICK_LOOP_CL8L(clicks_avx512z_c8l,SS_V5,win_masks_avx512z,vlane::clscan8_v5,lane_live_avx512,vcl2::cdec8_v5,vcl2,SS_CL8_CONST2)
SS_CLICK_LOOP_CL8L(clicks_avx512y_c8l,SS_V5,win_masks_avx512y,vlane::clscan8_v5,lane_live_avx512,vcl2::cdec8_v5,vcl2,SS_CL8_CONST2)
// ==== lkg: link lines on the general route (IDs are opaque texts) ====
// A link line is two ID texts. The links stage wants, per ID, its key for the ID table: a text of up to 11 bytes
// in the table's packed form (KeyTable::pack: every short ID of the data, 'u' + up to 8 characters, and any other
// short text), a longer one as KeyTable::Long (a canonical long ID - one byte + 32 hex digits - as two words, any
// other text as pointer + length), each with its hash and home slot. fill_* cut lines into a batch of such keys:
//   fill_v5    one 64-byte load per line, three byte compares -> newline / comma / quote masks, the rest is bit work:
//              takes every line of up to 63 bytes with one comma whose quotes wrap whole fields (so also the lines
//              with one or two long IDs), i.e. all generated lines but those with two long IDs and a quote
//   fill_base  two 16-byte loads (SSE2: any CPU, valgrind): takes the lines whose two fields are short
// A line a fill does not take goes through link_split, the line code of the old form, which decides everything else
// (three fields, empty fields, quotes inside a field, the last lines of a file). Accepting is exact: with exactly one
// comma before the newline, a field that begins with a quote ending with one and holding no other, and no other
// quote in the line, take_fields returns precisely the two texts cut here, both non-empty.
namespace lkg{
static constexpr int LB=16; // lines per batch
struct Batch{ // 2 IDs per line, in line order: IDs 2i and 2i+1 are one line
 alignas(64) uint64_t a[2*LB]; // k1 != 0: the packed key's k0 | k1 == 0: KeyTable::Long::a
 uint64_t b[2*LB];             // k1 == 0: KeyTable::Long::b
 uint64_t h[2*LB];             // k1 == 0: the key's hash (KeyTable::long_key)
 const char* s[2*LB];          // k1 == 0: the text (it becomes the name of a new ID)
 uint32_t len[2*LB];
 uint32_t k1[2*LB];            // the packed key's k1 (never 0); 0: a key that does not pack
 uint32_t kind[2*LB];          // k1 == 0: KeyTable::Long::kind
 uint32_t j[2*LB];             // home slot
 uint32_t id[2*LB];            // the IDs' numbers (set by the links stage)
 uint8_t lq[2*LB];             // which IDs have a long key (nlong of them)
 int n=0,nlong=0;              // IDs in the batch; long keys among them
 uint64_t fast=0,slow=0,longs=0; // totals: lines cut by the fill itself, lines through link_split, long keys
};
// A key that does not pack (over 11 bytes), out of line: one ID in eight.
static inline __attribute__((always_inline)) void put_long_body(Batch& B,int q,const char* s,size_t n,const KeyTable::Slot* slots,size_t mask){
 KeyTable::Long l;const uint64_t h=KeyTable::long_key(string_view(s,n),l);const uint32_t j=(uint32_t)(h&mask);
 B.a[q]=l.a;B.b[q]=l.b;B.h[q]=h;B.s[q]=s;B.len[q]=(uint32_t)n;B.k1[q]=0;B.kind[q]=l.kind;B.j[q]=j;__builtin_prefetch(&slots[j],0,1);
 B.lq[B.nlong++]=(uint8_t)q;++B.longs;
}
static __attribute__((noinline)) void put_long(Batch& B,int q,const char* s,size_t n,const KeyTable::Slot* slots,size_t mask){put_long_body(B,q,s,n,slots,mask);}
static inline __attribute__((always_inline)) void put(Batch& B,int q,const char* s,size_t n,const KeyTable::Slot* slots,size_t mask){
 uint64_t k0;uint32_t k1;
 if(__builtin_expect(KeyTable::pack(string_view(s,n),k0,k1),1)){
  const uint32_t j=(uint32_t)(KeyTable::hash_packed(k0,k1)&mask);
  B.a[q]=k0;B.k1[q]=k1;B.j[q]=j;__builtin_prefetch(&slots[j],1,1);return;
 }
 put_long(B,q,s,n,slots,mask);
}
// A line through link_split (out of line: the fills' own cut takes nearly every line).
static __attribute__((noinline)) const char* line_split(Batch& B,const char* p,const char* end,const KeyTable::Slot* slots,size_t mask){
 string_view f[2];const char* next;
 if(link_split(p,end,f,next)){put(B,B.n,f[0].data(),f[0].size(),slots,mask);put(B,B.n+1,f[1].data(),f[1].size(),slots,mask);B.n+=2;++B.slow;}
 return next;
}
// Both: lines from p on, as long as they begin before lim and the batch has room; returns where the next line begins.
static const char* fill_base(Batch& B,const char* p,const char* lim,const char* end,const KeyTable::Slot* slots,size_t mask){
 const __m128i vnl=_mm_set1_epi8('\n'),vc=_mm_set1_epi8(','),vq=_mm_set1_epi8('"');
 do{
  // The comma within the first 16 bytes, the newline within 16 bytes after it, no other comma, both fields 1..11 bytes.
  if(__builtin_expect(end-p>=32,1)){
   const __m128i x0=_mm_loadu_si128((const __m128i*)p);
   const uint32_t c0=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vc));
   if(__builtin_expect(c0!=0,1)){
    const unsigned P=(unsigned)__builtin_ctz(c0);const uint32_t mP=(1u<<P)-1;
    const uint32_t n0=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vnl))&mP,q0=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x0,vq))&mP;
    const __m128i x1=_mm_loadu_si128((const __m128i*)(p+P+1));
    const uint32_t n1=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vnl));
    if(__builtin_expect(n1!=0&&n0==0,1)){
     const unsigned L=(unsigned)__builtin_ctz(n1);const uint32_t mL=(1u<<L)-1;
     const uint32_t c1=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vc))&mL,q1=(uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(x1,vq))&mL;
     const uint32_t qa=q0&1u,qb=q1&1u;
     const unsigned na=P-2*qa,nb=L-2*qb; // (wraps to a huge number when a field is only a quote: refused below)
     const uint32_t wa=qa|(qa<<((P-1)&31)),wb=qb|(qb<<((L-1)&31));
     if(__builtin_expect(c1==0&&q0==wa&&q1==wb&&na-1<11&&nb-1<11,1)){
      put(B,B.n,p+qa,na,slots,mask);put(B,B.n+1,p+P+1+qb,nb,slots,mask);
      B.n+=2;++B.fast;p+=P+L+2;
      continue;
     }
    }
   }
  }
  p=line_split(B,p,end,slots,mask);
 }while(B.n<2*LB&&p<lim);
 return p;
}
#define SS_LKG5 __attribute__((target("avx2,bmi,bmi2,popcnt,lzcnt,avx512f,avx512bw,avx512vl")))
// KeyTable::pack for a text of 1..11 bytes, without the table of masks (BMI2); same k0 / k1.
SS_LKG5 static inline __attribute__((always_inline)) void put5(Batch& B,int q,const char* s,unsigned n,const KeyTable::Slot* slots,size_t mask){
 uint64_t lo;uint32_t hi;memcpy(&lo,s,8);memcpy(&hi,s+8,4);
 const uint64_t k0=_bzhi_u64(lo,8*n);                          // n >= 8: all 64 bits
 const uint32_t k1=_bzhi_u32(hi,8*(n>8?n-8:0))|((n+1)<<24);   // n <= 8: no byte of the second word
 const uint32_t j=(uint32_t)(KeyTable::hash_packed(k0,k1)&mask);
 B.a[q]=k0;B.k1[q]=k1;B.j[q]=j;__builtin_prefetch(&slots[j],1,1);
}
// put_long for fill_v5: the same text built with VEX encodings (a call from 512-bit code into SSE2 code would pay for
// the dirty upper halves on every instruction).
SS_LKG5 static __attribute__((noinline)) void put_long5(Batch& B,int q,const char* s,size_t n,const KeyTable::Slot* slots,size_t mask){put_long_body(B,q,s,n,slots,mask);}
alignas(64) static char FILL_C[3][64]; // 64 x '\n', ',', '"' (filled once; read from memory so that no register has to hold them across the calls)
SS_LKG5 static const char* fill_v5(Batch& B,const char* p,const char* lim,const char* end,const KeyTable::Slot* slots,size_t mask){
 int n=B.n;uint64_t fast=0;
 do{
  if(__builtin_expect(end-p>=64,1)){
   const __m512i x=_mm512_loadu_si512((const void*)p);
   const uint64_t mn=_mm512_cmpeq_epi8_mask(x,_mm512_load_si512((const void*)FILL_C[0]));
   if(__builtin_expect(mn!=0,1)){
    const unsigned L=(unsigned)_tzcnt_u64(mn); // the line's length, at most 63
    const uint64_t mc=_bzhi_u64(_mm512_cmpeq_epi8_mask(x,_mm512_load_si512((const void*)FILL_C[1])),L);
    const uint64_t mq=_bzhi_u64(_mm512_cmpeq_epi8_mask(x,_mm512_load_si512((const void*)FILL_C[2])),L);
    if(__builtin_expect(mc!=0&&(mc&(mc-1))==0,1)){
     const unsigned P=(unsigned)_tzcnt_u64(mc); // the one comma; P < L
     unsigned na=P,nb=L-P-1;const char* sa=p;const char* sb=p+P+1;
     if(__builtin_expect(mq!=0,0)){
      // Quotes allowed: first and last byte of a field that begins with one.
      const uint64_t qa=mq&1u,qb=(mq>>(P+1))&1u;
      const uint64_t want=qa|(qa<<((P-1)&63))|(qb<<(P+1))|(qb<<((L-1)&63));
      if(mq!=want)goto general;
      na-=2*(unsigned)qa;nb-=2*(unsigned)qb;sa+=qa;sb+=qb; // (a field that is one quote wraps to a huge length: refused below)
     }
     if(__builtin_expect(na-1<63&&nb-1<63,1)){
      if(__builtin_expect(na<=11,1))put5(B,n,sa,na,slots,mask);else put_long5(B,n,sa,na,slots,mask);
      if(__builtin_expect(nb<=11,1))put5(B,n+1,sb,nb,slots,mask);else put_long5(B,n+1,sb,nb,slots,mask);
      n+=2;++fast;p+=L+1;
      continue;
     }
    }
   }
  }
  general:
  B.n=n;p=line_split(B,p,end,slots,mask);n=B.n;
 }while(n<2*LB&&p<lim);
 B.n=n;B.fast+=fast;
 return p;
}
static inline bool has_v5(){
 static const bool h=[]{memset(FILL_C[0],'\n',64);memset(FILL_C[1],',',64);memset(FILL_C[2],'"',64);
  return __builtin_cpu_supports("avx512f")&&__builtin_cpu_supports("avx512bw")&&__builtin_cpu_supports("avx512vl")&&__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("lzcnt")&&__builtin_cpu_supports("popcnt");}();
 return h;
}
} // namespace lkg
// ==== end of lkg ====
// ---- cl8g: the click loop of the GENERAL route (opaque IDs, GFAST.md package C): lanes as above, kernel vclg::cl8g ----
// Per turn: group N is scanned (three windows per row; a row whose line end is not certain goes through lane_fix),
// group D decoded and the ID table slot of each of its users asked for, group F finished: campaign lookup, the user's
// number (the slot is in cache by now; a new user is added exactly as the line code adds it, and only for a row that
// has its campaign), the record appended to its click bucket. A row that is neither ok nor drop goes through
// click_line_v3<false> at that point (clicks need no line order: copies of a click are identical); its pending user
// is resolved at once, so no record of this loop is ever appended to a bucket that a waiting pointer looks into.
// SS_CLG=0: the loop with the line index (click_line_v3 for every line), as before. SS_CLG=check: every ok / drop
// row against the portable reading (clg_check_row). SS_CLG=ref: the scalar twin of the kernel (no AVX-512 needed).
static unsigned optClg=1; // 0 off, 1 on, 2 check, 3 scalar twin, 4 scalar twin + check
struct alignas(64) ClgAsk{uint64_t h[8];}; // per ok row: KeyTable::hash_packed of the user words, or long_key's hash (the slot is h & mask)
static constexpr uint64_t CLG_LONGK=(uint64_t)(0x100u|(unsigned)'u')*0x9E3779B97F4A7C15ull; // KeyTable::long_key: kind * constant
static inline __attribute__((always_inline)) void clg_ask(const vclg::Cl8gOut& o,unsigned rows,ClgAsk& a,const KeyTable& tab){
 for(unsigned r=o.ok&rows;r;r&=r-1){const unsigned i=(unsigned)__builtin_ctz(r);
  const uint64_t cls=o.ucls[i];
  const uint64_t hp=KeyTable::hash_packed(o.uk0[i],(uint32_t)o.uk1[i]),hl=mix64(o.ulo[i]^mix64(o.uhi[i]^CLG_LONGK));
  uint64_t h=cls?hl:hp;
  if(__builtin_expect(cls==vclg::CLS_OTHER,0)){KeyTable::Long key;h=KeyTable::long_key(string_view(o.us[i],(size_t)o.ul[i]),key);}
  a.h[i]=h;__builtin_prefetch(&tab.slots[h&tab.mask],0,1);}
}
// The user number of ok row i (asked for by clg_ask a turn ago).
static inline __attribute__((always_inline)) uint32_t clg_user(const vclg::Cl8gOut& o,unsigned i,const ClgAsk& a,KeyTable& tab,ThreadState& tl){
 const auto R_=memory_order_relaxed;
 const uint64_t h=a.h[i];const size_t j=h&tab.mask;uint32_t g;
 if(__builtin_expect(o.ucls[i]==vclg::CLS_SHORT,1)){
  const uint64_t k0=o.uk0[i];const uint32_t k1=(uint32_t)o.uk1[i];KeyTable::Slot& e=tab.slots[j];
  if(__builtin_expect(e.k1.load(memory_order_acquire)==k1&&e.k0.load(R_)==k0,1))return e.v.load(R_);
  g=tab.find_or_add_packed(k0,k1,j,string_view(o.us[i],(size_t)o.ul[i]));
 }else{
  const string_view u(o.us[i],(size_t)o.ul[i]);KeyTable::Long key;
  if(o.ucls[i]==vclg::CLS_LONG){key.a=o.ulo[i];key.b=o.uhi[i];key.kind=0x100u|(unsigned)'u';}else KeyTable::long_key(u,key);
  g=tab.find_or_add_long_key(key,h,u);
 }
 return g!=KeyTable::NONE?g:(LOCAL_USER|tl.users.intern(string_view(o.us[i],(size_t)o.ul[i])));
}
// SS_CLG=check: row i of a group against the portable reading of its line (scan_line + take_fields and the text of
// on_click). ok: the same four fields, time, campaign and user number; drop: on_click makes no click of the line.
[[gnu::noinline]] static void clg_check_row(const vclg::Cl8gOut& o,unsigned i,const char* p,const char* next,const char* end,bool isOk,bool found,uint32_t ci,uint32_t user,const KeyTable& tab,const KeyTable& camps,int64_t clickStart){
 const char* comma[4];int nc,nq;const char* le;string_view f[4];
 const char* const nx=scan_line(p,end,comma,4,nc,nq,le);
 const bool gen=take_fields(p,le,comma,nc,nq,4,f);
 int64_t ts=0;uint32_t cg=KeyTable::NONE;
 const bool click=gen&&nx==next&&!f[0].empty()&&!f[1].empty()&&parse_ts_fast(f[3],ts)&&ts>=clickStart&&ts<window_end;
 const char* why=nullptr;
 if(nx!=next)why="line end";
 else if(!isOk){if(click)why="a dropped line is a click";}
 else if(!click)why="an ok row is not a click";
 else{
  cg=camps.find(f[2]);
  if(f[0].data()!=o.ks[i]||f[0].size()!=o.kl[i])why="click_id text";
  else if(f[1].data()!=o.us[i]||f[1].size()!=o.ul[i])why="user_id text";
  else if(f[2].data()!=o.sc[i]||f[2].size()!=o.lc[i])why="campaign text";
  else if(o.ckh[i]!=(uint64_t)(ts-clickStart)<<16)why="time";
  else if(found!=(cg!=KeyTable::NONE)||(found&&ci!=cg))why="campaign number";
  else if(found&&!(user&LOCAL_USER)&&tab.find(f[1])!=user)why="user number";
  else{ // the ID classes and words, by a plain reading
   const string_view id=f[0],us=f[1];
   const auto low=[](string_view t,char top){for(char ch:t)if(!((ch>='0'&&ch<='9')||(ch>='a'&&ch<=top)))return false;return true;};
   uint64_t cls=2,key=(uint64_t)(uintptr_t)id.data(),hi=0;
   if(id[0]=='k'&&id.size()<=9&&low(id.substr(1),'z')){cls=0;key=0;memcpy(&key,id.data()+1,id.size()-1);}
   else if(id[0]=='k'&&id.size()==33&&low(id.substr(1),'f')){cls=1;hi=strtoull(string(id.substr(1,16)).c_str(),nullptr,16);key=strtoull(string(id.substr(17,16)).c_str(),nullptr,16);}
   if(o.kcls[i]!=cls||o.kkey[i]!=key||o.khi[i]!=hi)why="click_id class or key";
   uint64_t uc=2,k0=0,lo=0,uh=0;uint32_t k1=0;
   if(KeyTable::pack(us,k0,k1))uc=0;
   else if(us[0]=='u'&&us.size()==33&&low(us.substr(1),'f')){uc=1;uh=strtoull(string(us.substr(1,16)).c_str(),nullptr,16);lo=strtoull(string(us.substr(17,16)).c_str(),nullptr,16);}
   if(o.ucls[i]!=uc||o.uk0[i]!=k0||o.uk1[i]!=k1||o.ulo[i]!=lo||o.uhi[i]!=uh)why="user_id class or key";
  }
 }
 if(!why)return;
 fprintf(stderr,"ERROR SS_CLG=check: %s: %.*s\n",why,(int)min<ptrdiff_t>(le-p,200),p);_exit(1);
}
// One ok row of group F.
#define SS_CLG_ROW(O_,K_,i_) {uint32_t ci_=0;const bool f_=camp_find(lp.cf,lp.camps,O_.sc[i_],(size_t)O_.lc[i_],ci_);uint32_t g_=0; \
   if(__builtin_expect(f_,1)){g_=clg_user(O_,i_,K_,tab,tl); \
    CRec r_;r_.id=O_.ks[i_];r_.idlen=(uint32_t)O_.kl[i_];r_.h=hkey_v3(r_.id,r_.idlen);r_.key=O_.ckh[i_]|(uint16_t)ci_;r_.user=g_; \
    Bucket<CRec>& vec_=lp.buckets[(size_t)(((r_.h>>40)*PC)>>24)]; \
    if(__builtin_expect(vec_.full(),0))vec_.reserve(max<size_t>(16,2*vec_.capacity())); \
    vec_.append(r_);} \
   if(__builtin_expect(chk_,0))clg_check_row(O_,i_,L_[i_],X_[i_],end,true,f_,ci_,g_,tab,camps,clickStart);}
#define SS_CLG_FINISH(O,K,LS,NX,M,MASKS) {const auto& O_=(O);const auto& K_=(K);const char* const* const L_=(LS);const char* const* const X_=(NX);const unsigned m_=(M); \
  const unsigned ok_=O_.ok&m_; \
  if(__builtin_expect(ok_==0xFFu,1)){rows+=8;SS_CLG_ROW(O_,K_,0) SS_CLG_ROW(O_,K_,1) SS_CLG_ROW(O_,K_,2) SS_CLG_ROW(O_,K_,3) SS_CLG_ROW(O_,K_,4) SS_CLG_ROW(O_,K_,5) SS_CLG_ROW(O_,K_,6) SS_CLG_ROW(O_,K_,7)} \
  else{rows+=(unsigned)__builtin_popcount(m_); \
   for(unsigned r_=m_;r_;r_&=r_-1){const unsigned i_=(unsigned)__builtin_ctz(r_); \
    if((ok_>>i_)&1)SS_CLG_ROW(O_,K_,i_) \
    else if((O_.drop>>i_)&1){if(__builtin_expect(chk_,0))clg_check_row(O_,i_,L_[i_],X_[i_],end,false,false,0,0,tab,camps,clickStart);} \
    else{const char* const q_=L_[i_];++slow;click_line_v3<false>(q_,MASKS(q_),nullptr,lp,pend,pp,dmax,send,ring,rpos,general);resolve_users(pend,pp,tab,tl);pp=pend;}}} }
#define SS_CLICK_LOOP_CLG(NAME,TARGET,MASKS,LSCAN,LIVE,DEC) \
template<class GEN> TARGET static void NAME(const char* base,size_t len,size_t from,size_t to,ThreadState& tl,KeyTable& tab,const KeyTable& camps,size_t PC,int64_t clickStart,GEN&& general){ \
 const char* p;const char* lim;if(!chunk_lines(base,len,from,to,p,lim))return; \
 if(p>=lim)return; \
 const char* const end=base+len;const bool chk_=optClg==2||optClg==4; \
 const ClLoop lp(tl,tab,camps,PC,end,clickStart); \
 PendUser pend[V3_BATCH];PendUser* pp=pend;uint32_t dmax=0;uint32_t send=0;CRing* const ring=nullptr;unsigned rpos=0;uint64_t rows=0,slow=0,shortG=0,fixed=0; \
 const vclg::ClgConst cc{clickStart,lp.wlen,vcl2::make_dates_cl(clickStart,window_end,window_start)}; \
 vclg::Cl8gOut out[2];vclg::Cl8gScan sc[2];ClgAsk ask[2]; \
 alignas(64) const char* A[4][8];alignas(64) const char* le[8];const char* tail=nullptr; \
 {const char* cut_[9];lane_cut(p,lim,end,cut_);for(unsigned i_=0;i_<8;++i_){A[0][i_]=cut_[i_];le[i_]=cut_[i_+1];}} \
 const char* const* lD=A[0];const char* const* xD=A[0];const char* const* lF=A[0];const char* const* xF=A[0]; \
 unsigned g=0,nD=0,nF=0;vclg::Cl8gScan* sN=&sc[0];vclg::Cl8gScan* sD=&sc[1];vclg::Cl8gOut* oD=&out[0];vclg::Cl8gOut* oF=&out[1];ClgAsk* kD=&ask[0];ClgAsk* kF=&ask[1];bool more=true; \
 do{ \
  unsigned nN=0;const char** const lN=A[g&3];const char** const xN=A[(g+1)&3]; \
  if(more){ \
   const unsigned live_=LIVE(lN,le); \
   if(__builtin_expect(live_==0xFFu,1)){nN=0xFFu;const unsigned unc_=LSCAN(lN,xN,*sN);if(__builtin_expect(unc_!=0,0))nN&=~lane_fix(lN,xN,unc_,end,tail,fixed);SS_LANE_PF1(xN)} \
   else if(live_){nN=live_;++shortG;const unsigned unc_=LSCAN(lN,xN,*sN)&live_;if(unc_)nN&=~lane_fix(lN,xN,unc_,end,tail,fixed); \
    for(unsigned i_=0;i_<8;++i_)if(!((live_>>i_)&1))xN[i_]=lN[i_];} /* a lane that is done stays where it is */ \
   else more=false; \
  } \
  if(nD){DEC(lD,xD,*sD,cc,*oD);clg_ask(*oD,nD,*kD,tab);} \
  if(nF)SS_CLG_FINISH(*oF,*kF,lF,xF,nF,MASKS) \
  lF=lD;xF=xD;nF=nD;lD=lN;xD=xN;nD=nN;++g; \
  {vclg::Cl8gScan* const t_=sN;sN=sD;sD=t_;vclg::Cl8gOut* const u_=oD;oD=oF;oF=u_;ClgAsk* const v_=kD;kD=kF;kF=v_;} \
 }while(more||nD||nF); \
 if(tail)line_general<4>(tail,end,general); /* the last line has no newline */ \
 tl.clgRows+=rows;tl.clgSlow+=slow;tl.laneShort+=shortG;tl.laneFixC+=fixed; \
}
// The avx2 instance is the stand-in without AVX-512 (scalar twin; callgrind, ASan): the same scan rule from win_masks_avx2.
SS_V3 [[gnu::noinline]] static unsigned lane_scan_clg_avx2(const char* const* ls,const char** nx,vclg::Cl8gScan&){
 unsigned unc=0;
 for(unsigned i=0;i<8;++i){const char* const p=ls[i];const WinMasks a=win_masks_avx2(p);
  const uint64_t e0=_tzcnt_u64(a.c);const char* const bb=p+e0+1;const WinMasks b=win_masks_avx2(bb);
  const uint64_t e1=_tzcnt_u64(b.c);const char* const bc=bb+e1+1;const WinMasks c=win_masks_avx2(bc);
  const uint64_t e3=_tzcnt_u64(c.n);nx[i]=bc+e3+1;
  if(((a.n&_blsmsk_u64(a.c))|(b.n&_blsmsk_u64(b.c))|((e0|e1|e3)>>6))!=0)unc|=1u<<i;}
 return unc;
}
[[gnu::noinline]] static void clg_dec_ref(const char* const* ls,const char* const* nx,const vclg::Cl8gScan&,const vclg::ClgConst& c,vclg::Cl8gOut& o){vclg::cl8g_ref(ls,nx,c,o);}
SS_CLICK_LOOP_CLG(clicks_avx2_cg,SS_V3,win_masks_avx2,lane_scan_clg_avx2,lane_live_scalar,clg_dec_ref)
SS_CLICK_LOOP_CLG(clicks_avx512z_cg,SS_V5,win_masks_avx512z,vclg::cscang8_v5,lane_live_avx512,vclg::cdecg8_v5)
static void run(const fs::path& data,const fs::path& scratch,const fs::path& output){
 auto __time0=chrono::steady_clock::now();
 // With SS_STATS=1 every TIME line is followed by a CPU line: processor time (all threads, user+system) of that stage.
 const bool __cpuLines=getenv("SS_STATS")!=nullptr;
 auto __cpu_now=[]{timespec t;clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t);return (double)t.tv_sec+1e-9*(double)t.tv_nsec;};
 double __cpu0=__cpu_now();
 // ... and by a SYS line (system CPU of the whole process in that stage, seconds) and a FLT line (minor page faults).
 // SYS/FLT EXITPREP at the very end cover what runs after the last mark.
 // ... and a CSW line: context switches of the whole process in the stage, voluntary (a thread went to sleep: futex,
 // lock or I/O wait) and involuntary (a thread was taken off its processor).
 auto __ru_now=[](double& sys,long& flt,long& vol,long& invol){rusage r{};getrusage(RUSAGE_SELF,&r);sys=(double)r.ru_stime.tv_sec+1e-6*(double)r.ru_stime.tv_usec;flt=r.ru_minflt;vol=r.ru_nvcsw;invol=r.ru_nivcsw;};
 double __sys0=0;long __flt0=0,__vol0=0,__inv0=0;if(__cpuLines)__ru_now(__sys0,__flt0,__vol0,__inv0);
 auto __sysflt=[&](const char* label){double s;long f,v,iv;__ru_now(s,f,v,iv);cerr<<"SYS "<<label<<" "<<s-__sys0<<"\nFLT "<<label<<" "<<f-__flt0<<"\nCSW "<<label<<" "<<v-__vol0<<" "<<iv-__inv0<<"\n";__sys0=s;__flt0=f;__vol0=v;__inv0=iv;};
 auto __mark=[&](const char *label){auto now=chrono::steady_clock::now();cerr<<"TIME "<<label<<" "<<chrono::duration<double>(now-__time0).count()<<"\n";__time0=now;
  if(__cpuLines){const double c=__cpu_now();cerr<<"CPU "<<label<<" "<<c-__cpu0<<"\n";__cpu0=c;__sysflt(label);}};
 // SS_FINE=1: FINE lines, the time since the start of run() at points inside the stages (the split of the serial
 // work before the parse stage, the side thread, the campaign file). Measuring only.
 static const bool fineLines=getenv("SS_FINE")!=nullptr;
 static const auto fineT0=chrono::steady_clock::now();
 const auto __fine=[](const char* label){if(fineLines)fprintf(stderr,"FINE %s %.6f\n",label,chrono::duration<double>(chrono::steady_clock::now()-fineT0).count());};
 optHuge=!getenv("SS_NOHUGE");optPrefault=getenv("SS_PREFAULT")!=nullptr;optAsyncFree=getenv("SS_ASYNCFREE")!=nullptr;
 if(const char* v=getenv("SS_FILEALIGN")){const string_view w(v);optFileAlign=w=="2m"?1:w=="2m+huge"?2:0;}
 if(const char* v=getenv("SS_OUTOPEN"))optOutEarly=string_view(v)=="early";
 if(const char* v=getenv("SS_OUTWRITE"))optOutEnd=string_view(v)=="end";
 if(const char* v=getenv("SS_FILEADVISE")){const string_view w(v);optFileAdvise=w=="random"?1:w=="seq"?2:0;}
 if(const char* v=getenv("SS_POPULATE")){const string_view w(v);optPopulate=w=="ahead"?1:w=="all"?2:w.substr(0,2)=="bg"?3:0;
  if(optPopulate==3&&w.size()>2){const int n=atoi(v+2);if(n>=1&&n<=64)optPopulateBg=n;}}
 if(__cpuLines)cerr<<"STATS kernel switches: SS_FILEALIGN="<<(optFileAlign==2?"2m+huge":optFileAlign?"2m":"off")<<" SS_POPULATE="<<(optPopulate==1?"ahead":optPopulate==2?"all":optPopulate==3?"bg"+to_string(optPopulateBg):string("off"))
  <<" SS_FILEADVISE="<<(optFileAdvise==1?"random":optFileAdvise==2?"seq":"off")<<" SS_OUTOPEN="<<(optOutEarly?"early":"off")<<" SS_OUTWRITE="<<(optOutEnd?"end":"off")<<" huge="<<optHuge<<" prefault="<<optPrefault<<" asyncfree="<<optAsyncFree<<" willneed="<<(getenv("SS_NOWILLNEED")?"off":(getenv("SS_WILLNEED")&&string_view(getenv("SS_WILLNEED"))=="inline")?"inline":"bg")<<" earlyunmap="<<!getenv("SS_NOEARLYUNMAP")<<"\n";
 auto manifest=read_all(data/"manifest.json");
 if(!parse_ts(json_string(manifest,"window_start"),window_start)||!parse_ts(json_string(manifest,"window_end"),window_end)||window_end<=window_start)throw runtime_error("Bad manifest");
 ingestBase=(int64_t)((uint64_t)window_start-(1ull<<38));
 int nday=(int)((window_end-window_start+DAY-1)/DAY);
 if(nday<=0||nday>1000)throw runtime_error("Unreasonable window length");
 // ---- Products and campaigns: two small files that depend on nothing else. They are read by a side thread
 // while this one maps the inputs and the link stages run (about 9 million instructions that used to sit, on one
 // thread, in front of the parse stage). The tables are only used after the join below, where a failure of the
 // side thread is thrown again. SS_CATALOG=inline: read them on this thread at that place, as before.
 FastMap<uint16_t> categories;vector<string> catnames;
 KeyTable products(1<<17); // product_id -> category number
 vector<Camp> campaigns;KeyTable camps(1<<13); // campaign_id -> campaign number
 uint16_t organic=0;
 const auto load_catalog=[&]{
 cat_id("unknown",categories,catnames);
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
 read_pairs(data/"campaigns.csv",[&](const string_view* f,bool ok){
  if(!ok)throw runtime_error("Malformed clean campaigns.csv");
  if(campaigns.size()>=65534)throw runtime_error("Too many campaigns");
  const uint32_t n=(uint32_t)campaigns.size();if(camps.intern(f[0],n)==n)campaigns.push_back({string(f[0]),string(f[1])});});
 {const char* const v=getenv("SS_CAMP");const string_view w(v?v:""); // table (default) | hash | check
  campCheck=w=="check";
  vector<CampEnt> ks;for(size_t i=0;i<campaigns.size();++i)if(campaigns[i].id.size()<=7)ks.push_back({camp_key((campaigns[i].id+string(8,'\0')).data(),campaigns[i].id.size()),i});
  campTable.keys=ks.size();if(w!="hash")campTable.build(ks);
  if(campCheck)campTable.look.lim=0;} // the loops take the KeyTable path, which compares
 organic=(uint16_t)campaigns.size();campaigns.push_back({"organic","organic"});
 };
 const bool catalogBg=getenv("SS_CATALOG")&&string_view(getenv("SS_CATALOG"))=="thread"; // default inline: on the scoring machine the side thread made KEYS wait 13-18 ms (diagnostic 17)
 exception_ptr catalogErr;thread catalogThread;
 struct Joiner{thread& t;~Joiner(){if(t.joinable())t.join();}} catalogJoiner{catalogThread}; // an exception on the way must not meet a running thread
 if(catalogBg)catalogThread=thread([&]{try{load_catalog();}catch(...){catalogErr=current_exception();}__fine("catalog-read-end");});
 __fine("start");
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
 __fine("map-events");
 add_files(data/"clicks",true,clickBytes,clickFiles);
 __fine("map-clicks");
 // Lines per byte, measured on the start of the largest event file, sizes the bucket buffers tightly.
 uint64_t estEvents=eventBytes/70;
 // (The count is a plain sum over the bytes, so it is taken in pieces by all threads: the same number as one
 // pass, which costs this thread the first touch and a scan of 4 MB. SS_SAMPLE=serial: one pass, as before.)
 if(sampleLen>4096){const size_t n=min<size_t>(sampleLen,size_t(4)<<20);uint64_t nl=0;
  static constexpr size_t SAMPLE_PIECE=size_t(256)<<10;
  if(T==1||n<=SAMPLE_PIECE||(getenv("SS_SAMPLE")&&string_view(getenv("SS_SAMPLE"))=="serial"))nl=(uint64_t)count_newlines(sampleBase,n);
  else{
   #pragma omp parallel for num_threads(T) schedule(static,1) reduction(+:nl)
   for(size_t at=0;at<n;at+=SAMPLE_PIECE)nl+=(uint64_t)count_newlines(sampleBase+at,min(SAMPLE_PIECE,n-at));
  }
  if(nl>16)estEvents=(uint64_t)((double)eventBytes*(double)nl/(double)n)+1;}
 const uint64_t estClicks=clickBytes/36;
 // SS_POPULATE=all|bg: the input's pages are entered into the page tables by madvise calls, a chunk at a time.
 const auto populate_chunk=[&chunks](size_t c){const Chunk& ck=chunks[c];if(ck.from<ck.len)madvise((void*)(ck.base+ck.from),min(ck.to,ck.len)-ck.from,MADV_POPULATE_READ);};
 static atomic<size_t> populateNext{0};static atomic<bool> populateStop{false};
 if(optPopulate==2){
  #pragma omp parallel for num_threads(T) schedule(dynamic,1)
  for(size_t c=0;c<chunks.size();++c)populate_chunk(c);
 }
 __mark("MAP");
 struct EarlyOut{vector<int> fd;int campFd=-1;bool dirs=false;atomic<bool> done{false};};
 shared_ptr<EarlyOut> earlyOut;
 if(optOutEarly){earlyOut=make_shared<EarlyOut>();earlyOut->fd.assign((size_t)T,-1);
  thread([eo=earlyOut,dir=output/"user_daily",cdir=output/"campaign_daily",T]{
   std::error_code ec,ec2;fs::create_directories(dir,ec);fs::create_directories(cdir,ec2);eo->dirs=!ec&&!ec2;
   {int f=::open((cdir/"part-00000.parquet").c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);if(f>=0&&::write(f,"PAR1",4)!=4){::close(f);f=-1;}eo->campFd=f;}
   for(int t=0;t<T;++t){char name[32];snprintf(name,sizeof name,"part-%05d.parquet",t);int f=::open((dir/name).c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);
    if(f>=0&&::write(f,"PAR1",4)!=4){::close(f);f=-1;} // the file's first four bytes go out here too; on any failure the worker creates the file itself
    eo->fd[(size_t)t]=f;}
   eo->done.store(true,memory_order_release);}).detach();}
 if(optPopulate==3)for(int i=0;i<optPopulateBg;++i)thread([&chunks,populate_chunk]{ // chunks outlives the threads' work: they stop before PARSE ends and the process leaves through _exit
  for(;;){if(populateStop.load(memory_order_relaxed))break;const size_t c=populateNext.fetch_add(1,memory_order_relaxed);if(c>=chunks.size())break;populate_chunk(c);}}).detach();
 // ---- Dense IDs: find the generator's keys from the first lines of one event file and one click file. ----
 // The route is OFF unless SS_DENSE=1 asks for it: IDs are opaque texts by the rules (one in eight is 32 hex digits, the
 // rest come out of a keyed bijection), so no key exists to be found and the search would only cost time.
 // (SS_NODENSE=1 keeps it off whatever SS_DENSE says.)
 if(getenv("SS_DENSE")&&atoi(getenv("SS_DENSE"))!=0&&!getenv("SS_NODENSE")&&sampleLen){
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
  __fine("key-samples");
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
 // SS_LINKS: how the first pass over the link files finds the simple lines (dense users only; see the pass).
 //   (unset)  eight lanes + the AVX-512 kernel vlk::lk8_v5 where the CPU has it (not under SS_ISA=sse2); else as "old"
 //   old      one line after the other through link_split + link_dense
 //   ref      eight lanes + the kernel's scalar twin vlk::lk8_ref (any CPU; for tests and instruction counts)
 //   check / refcheck  as the default / as ref, and every line the kernel takes is compared with what link_split +
 //            link_dense say about it (abort on a difference); counts under SS_STATS
 //   cas      as the default, but every union is applied at once with comp_union (compare-and-swap), one kernel call
 //            behind its prefetch, instead of going through the bucket lists
 const char* const lkMode=getenv("SS_LINKS");const string_view lkm(lkMode?lkMode:"");
 const bool lkRef=lkm=="ref"||lkm=="refcheck",lkCheck=lkm=="check"||lkm=="refcheck",lkCas=lkm=="cas";
 const bool lkWanted=lkm!="old"&&(lkRef||(vlk::lk_has512()&&(lkMode||!(getenv("SS_ISA")&&string_view(getenv("SS_ISA"))=="sse2"))));
 // Link files are handed out in pieces of 256 KB when the lane loop runs them (2 MB otherwise, as before): a piece is
 // about 12,000 lines, cheap enough now that the threads' shares of a small link folder should be even (20M events:
 // 18 MB of links = 13 pieces of 2 MB for 8 threads, or 72 of 256 KB). SS_LINKCH=bytes sets the size (measuring).
 size_t linkCH=lkWanted?(size_t(256)<<10):CH;
 if(const char* v=getenv("SS_LINKCH")){const unsigned long long x=strtoull(v,nullptr,10);if(x>=64&&x<=(1ull<<30))linkCH=(size_t)x;}
 for(const Mapped& m:map_files(csv_files(data/"links"),linkFiles)){if(!m.len)continue;
  ++linkLines; // a last line without '\n'
  linkBytes+=m.len;++linkFileCount;
  for(size_t from=0;from<m.len;from+=linkCH)linkChunks.push_back({m.base,m.len,from,from+linkCH,false});}
 __fine("map-links");
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
 // With dense users only the IDs that cannot take the dense route are new to the ID table (generated data
 // has a few hundred of them at 20M events), so an eighth of the room is still several hundred times what is
 // needed - and the table, whose slots are touched at random, is an eighth as large: its first touches
 // (whole huge pages, zeroed) are most of the LINKREAD stage. More new IDs than that take the fallback
 // path as before. SS_ROOMY_IDS=1 keeps the full room.
 if(denseUsers&&!getenv("SS_TEST_MAXNEW")&&!getenv("SS_ROOMY_IDS"))maxNew64=maxNew64/8+4096;
 if(denseUsers)denseComp=(atomic<uint32_t>*)map_huge((dense.limI+1)*sizeof(uint32_t));
 dparse.users=denseUsers;
 if(!denseUsers)dparse.events=dparse.clicks=false; // the record loops decode both IDs of a line together
 if(!dparse.events&&!dparse.clicks)dparse.bucket=false;
 // SS_DENSE_USERS=code: the line loops decode the user ID of every dense-record event line themselves (default:
 // raw, the text goes into the slot and is decoded once per run of equal users after parsing; slot form only).
 {const char* v=getenv("SS_DENSE_USERS");dparse.raw=dparse.events&&!dparse.bucket&&!(v&&string_view(v)=="code");}
 // SS_COMMIT=direct (default) | stream: where the parse stage puts a dense event row (slot form). direct = into its
 // slot (eslot_commit); stream = behind the stream of its bucket, and the SCATTER step fills the slots (see
 // DenseParse, StreamStore). SS_BSH = log2 of the counters per SCATTER turn (4-20, default 14: 512 KB of slots);
 // the streams are cut at min(SS_BSH, 16) bits, and at more if the limit would give over 2^20 buckets.
 // SS_BCH = log2 of the bytes per stream chunk (6-16, default 12; made smaller if all buckets' first chunks would
 // be a large part of the expected rows, so tiny buckets on small data do not eat memory).
 {const char* v=getenv("SS_COMMIT");dparse.stream=dparse.events&&!dparse.bucket&&v&&string_view(v)=="stream";
  if(const char* b=getenv("SS_BSH")){const int x=atoi(b);if(x>=4&&x<=20)dparse.bwin=(unsigned)x;}
  if(const char* b=getenv("SS_BCH")){const int x=atoi(b);if(x>=6&&x<=16)dparse.bch=(unsigned)x;}
  if(const char* b=getenv("SS_PUT"))dparse.put8=string_view(b)!="row"; // SS_PUT=row: the ev8 loops append row by row (default on AVX-512: a kernel group at once, estream_put8_v5)
  dparse.bsh=min(dparse.bwin,16u);
  while(dparse.bsh<16&&(dense.limE>>dparse.bsh)>=(uint64_t(1)<<20))++dparse.bsh;
  dparse.bwin=max(dparse.bwin,dparse.bsh);
  // SS_SPF=0|1: the ev8 loops ask for a row's fill line and bucket state a group ahead (default: from 1,000 buckets,
  // where a thread's lines and states are over 80 KB. CPU of the parse stage, one thread, this box: dev at 1,183 /
  // 4,730 / 9,460 buckets 43.5 -> 42.6, 47.0 -> 43.9, 50.6 -> 46.4 ms; big20 at 1,221 buckets 1058 -> 1008 ms.
  // Not measured on the scoring machine.)
  dparse.spf=(dense.limE>>dparse.bsh)>=1000;if(const char* b=getenv("SS_SPF"))dparse.spf=b[0]=='1';}
 // Event lines with raw users use the two-phase loop (SS_EVENT_LOOP_2P) unless SS_DENSE_AHEAD is given: then the
 // loop with the ring of that length (0 = write at the end of the line) runs, for comparison.
 dparse.twoPhase=dparse.raw&&!getenv("SS_DENSE_AHEAD");
 // SS_LINES=batch: the batch event loop (SS_EVENT_LOOP_BATCH), for comparison while it is being built.
 dparse.batch=dparse.twoPhase&&getenv("SS_LINES")&&string_view(getenv("SS_LINES"))=="batch";
 // SS_LINES=ev8: the event loop that hands eight lines at a time to a kernel (SS_EVENT_LOOP_EV8). Where the CPU
 // has the AVX-512 kernel it is also the default (decided at the parse stage, see ev8Auto); SS_LINES=2p keeps the
 // two-phase loop there.
 dparse.ev8=dparse.twoPhase&&getenv("SS_LINES")&&string_view(getenv("SS_LINES"))=="ev8";
 // SS_CLICKS=cl8: the click loop that hands eight lines at a time to a kernel (SS_CLICK_LOOP_CL8; slot form only).
 // Where the CPU has the AVX-512 kernel it is also the default (see cl8Auto); any other SS_CLICKS value (SS_CLICKS=ix)
 // keeps the click loop with the line index (SS_CLICK_LOOP_IX). SS_CLICKS=cl8 with the avx2 masks runs the scalar twin.
 dparse.cl8=dparse.clicks&&!dparse.bucket&&getenv("SS_CLICKS")&&string_view(getenv("SS_CLICKS"))=="cl8";
 cerr<<"dense parse: users "<<dparse.users<<" event records "<<dparse.events<<" click records "<<dparse.clicks<<" bucket "<<dparse.bucket<<" raw users "<<dparse.raw<<" two-phase "<<dparse.twoPhase<<" batch "<<dparse.batch<<" ev8 "<<dparse.ev8<<" cl8 "<<dparse.cl8<<(dparse.stream?" commit stream bsh ":" commit direct")<<(dparse.stream?to_string(dparse.bsh)+" turn "+to_string(dparse.bwin)+" bch "+to_string(dparse.bch)+(dparse.put8&&vlk::lk_has512()?" put group":" put row")+(dparse.spf?" spf":""):string())<<"\n";
 // First pass over the link files.
 // Without dense users it only counts lines (the table and the union-find arrays are sized by them).
 // With dense users it is the only pass that reads the files: a "simple" line - two dense user IDs
 // with aliases 0-7 of one user index - is applied to that index's alias word at once; every other
 // line marks the dense indexes it names complex and is kept (its two fields) for the ID table.
 // A complex index keeps the general route for all its IDs, so the simple lines already applied to its
 // word have to reach the table too: they are replayed from the word afterwards (same groups).
 struct LinkGen{const char* a;const char* b;uint32_t la,lb;};
 static constexpr int LKR_MAX=64;static constexpr size_t LKB=size_t(1)<<16,LKG=1024; // most buckets; records per block of a bucket's list; kernel calls between two looks at the room left
 struct alignas(64) LinkOut{vector<LinkGen> gen;vector<uint32_t> cplx;uint32_t maxIdx=0;uint64_t nk=0,nslow=0,ntail=0,missed=0;double cpu=0,cpu2=0; // cpu2: of the second step; nk..: lane loop counts, cpu: thread seconds in the pass (SS_STATS)
  // Lane loop: the unions of the simple lines wait here as records (vlk::LkOut::rec), one list per bucket of user
  // indexes; bp = where the next record of a bucket goes (bp[buckets] is the place for lanes without a union).
  uint32_t* bp[LKR_MAX+1]={};uint32_t* bcap[LKR_MAX+1]={};uint32_t* bstart[LKR_MAX]={};vector<pair<uint32_t*,uint32_t*>> blocks[LKR_MAX];uint32_t trash[16];};
 vector<LinkOut> lout(denseUsers?T:0);
 {
  bool v3=__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("popcnt")&&__builtin_cpu_supports("lzcnt");
  if(const char* isa=getenv("SS_ISA"))if(string_view(isa)=="sse2")v3=false;
  void (*const link_dense)(const char*,size_t,const char*,size_t,LinkDense&)=v3?link_dense_v3:link_dense_base;
  // SS_LINKS (read where the link files are cut into pieces) decides how the simple lines are found.
  // The unions of the lane loop (default): a simple line with two different aliases becomes a 32-bit record in a list
  // of its thread, one list per bucket; a bucket is every lkR-th block of 4096 user indexes. When all chunks are done
  // each bucket is applied by one thread (vlk::lk_union_v5, sixteen records per turn; lk_union_ref under ref): no word
  // is changed by two threads, so no lock prefix, and a bucket's words are an lkR-th of the array. The result is the same word whatever the order: a word is "the
  // smallest alias of each alias's group", which the set of unions decides. (Unions from the line code - refused
  // lines, file tails - still go through comp_union at once; all of that is over before the buckets are applied.)
  const bool lkOn=denseUsers&&lkWanted;
  void (*const lk8)(const char**,const char* const*,const vlk::LkConst&,vlk::LkOut&)=lkRef?vlk::lk8_ref:vlk::lk8_v5;
  int lkR=32;while(lkR<T&&lkR<LKR_MAX)lkR*=2;
  static constexpr array<uint8_t,256> POP8=[]{array<uint8_t,256> a{};for(unsigned m=0;m<256;++m){unsigned n=0;for(unsigned j=0;j<8;++j)n+=(m>>j)&1;a[m]=(uint8_t)n;}return a;}();
  const vlk::LkConst lkc=vlk::lk_const(dense.ku,dense.limI,(uint64_t)lkR);
  static const bool lkStats=getenv("SS_STATS")!=nullptr;
  #pragma omp parallel num_threads(T)
  {
  #pragma omp for schedule(dynamic,1) reduction(+:linkLines)
  for(size_t c=0;c<linkChunks.size();++c){const Chunk& ck=linkChunks[c];
   if(!denseUsers){linkLines+=count_newlines(ck.base+ck.from,min(ck.to,ck.len)-ck.from);continue;}
   if(ck.from>=ck.len)continue;
   const char* const end=ck.base+ck.len;
   const char* nl=(const char*)memchr(ck.from?ck.base+ck.from-1:ck.base,'\n',ck.from?ck.len-(ck.from-1):ck.len);
   if(!nl)continue;
   const char* const lim=ck.base+min(ck.to,ck.len);
   LinkOut& out=lout[omp_get_thread_num()];
   struct CpuNote{LinkOut& o;double t0;~CpuNote(){if(lkStats)o.cpu+=thr_cpu_s()-t0;}} cpuNote{out,lkStats?thr_cpu_s():0.0};
   // Simple lines wait in a small batch so that their index's word is in the cache when it is changed.
   static constexpr int SB=8;
   struct Simple{uint32_t idx,aa,ab;} sb[SB];int ns=0;uint32_t maxIdx=out.maxIdx;
   const auto apply=[&]{for(int i=0;i<ns;++i)comp_union(sb[i].idx,sb[i].aa,sb[i].ab);ns=0;};
   // The line code: everything about the line that starts at p; returns where the next line starts.
   const auto one_line=[&](const char* p)__attribute__((always_inline))->const char*{
    string_view f[2];const char* next;
    const bool ok=link_split(p,end,f,next);
    if(!ok)return next;
    LinkDense r;link_dense(f[0].data(),f[0].size(),f[1].data(),f[1].size(),r);
    if(r.da&&r.db&&r.ia==r.ib&&r.aa<8&&r.ab<8){
     maxIdx=max(maxIdx,(uint32_t)r.ia);
     if(r.aa!=r.ab){__builtin_prefetch(&denseComp[r.ia],1,1);sb[ns++]={(uint32_t)r.ia,r.aa,r.ab};if(ns==SB)apply();}
     return next;
    }
    // Whoever turns an index's bit on notes the index for the replay (so each index is noted once).
    if(r.da&&!dense_set_complex(r.ia))out.cplx.push_back((uint32_t)r.ia);
    if(r.db&&!dense_set_complex(r.ib))out.cplx.push_back((uint32_t)r.ib);
    out.gen.push_back({f[0].data(),f[1].data(),(uint32_t)f[0].size(),(uint32_t)f[1].size()});
    return next;
   };
   if(!lkOn||ck.len<512){
    for(const char* p=nl+1;p<lim;)p=one_line(p);
    apply();out.maxIdx=maxIdx;
    continue;
   }
   // Eight lanes (SS_LINKS): the chunk's lines are cut into eight runs at line starts and each lane walks its own run,
   // one line per lane and kernel call. vlk::lk8 takes the simple lines (see its contract); a line it refuses, and the
   // last lines of a file (the kernel reads up to 96 bytes from a line start), go through one_line. The lines and what
   // is done for each are those of the loop above; only the order differs, and nothing here depends on the order
   // (a union is commutative; the lists are per thread and already depend on which thread gets which chunk).
   const char* s[9];s[0]=nl+1;s[8]=lim;
   {const size_t span=s[0]<lim?(size_t)(lim-s[0]):0;
    for(int j=1;j<8;++j){const char* const t=s[0]+span*(size_t)j/8; // t-1 >= nl: the byte before a lane start is a newline
     const char* const q=(const char*)memchr(t-1,'\n',(size_t)(end-(t-1)));
     s[j]=q&&q+1<lim?q+1:lim;}}
   const char* const kend=end-160; // a live lane has 160 readable bytes, so a lane that has just ended still has 96
   const char* kp[8];const char* klim[8];const char* tail[8];
   for(int j=0;j<8;++j){klim[j]=min(s[j+1],kend);
    if(s[j]<klim[j]){kp[j]=s[j];tail[j]=nullptr;}else{kp[j]=klim[j]=ck.base;tail[j]=s[j];}} // a lane that never runs reads the chunk's first bytes
   vlk::LkOut ob[2];memset(ob[0].mx,0,sizeof ob[0].mx);memset(ob[1].mx,0,sizeof ob[1].mx);
   vlk::LkOut* prev=nullptr;int cur=0;uint64_t nk=0,nslow=0;
   // Memory for records: its own mapping, small pages on purpose (a list is touched a few pages at a time; a huge page
   // under each of the 32 lists of a thread would be 2 MB of zeroing for a few kilobytes of records).
   const auto rec_block=[](size_t records)->uint32_t*{
    const size_t bytes=records*sizeof(uint32_t);
    void* const m=mmap(nullptr,bytes,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
    if(m==MAP_FAILED){cerr<<"ERROR out of memory\n";_exit(1);}
    madvise(m,bytes,MADV_NOHUGEPAGE);
    return (uint32_t*)m;};
   // Every LKG kernel calls: a bucket whose block could run full within the next LKG calls (8 records each at most)
   // gets a new block. The first blocks of a thread are one allocation, touched only as far as records are written.
   const auto refill=[&]()__attribute__((noinline,cold))->size_t{
    if(!out.bp[lkR]){uint32_t* const slab=rec_block((size_t)lkR*LKB);
     for(int r=0;r<lkR;++r){out.bstart[r]=out.bp[r]=slab+(size_t)r*LKB;out.bcap[r]=out.bp[r]+LKB;}
     out.bp[lkR]=out.trash;}
    for(int r=0;r<lkR;++r)if((size_t)(out.bcap[r]-out.bp[r])<8*LKG){
     out.blocks[r].push_back({out.bstart[r],out.bp[r]});
     uint32_t* const nb=rec_block(LKB);
     out.bstart[r]=out.bp[r]=nb;out.bcap[r]=nb+LKB;}
    return LKG;};
   size_t room=0;
   const auto unions=[&](const vlk::LkOut& o)__attribute__((always_inline)){
    for(unsigned i=0;i<8;++i)if((o.un>>i)&1)comp_union(o.idx[i],(uint32_t)o.aa[i],(uint32_t)o.ab[i]);};
   for(;;){
    vlk::LkOut& o=ob[cur];
    const char* was[8];if(__builtin_expect(lkCheck,0))memcpy(was,kp,sizeof was);
    lk8(kp,klim,lkc,o);
    if(!(o.ok|o.slow))break; // no lane is live any more: this call did nothing
    if(lkCas)for(int i=0;i<8;++i)__builtin_prefetch(&denseComp[o.idx[i]],1,1);
    if(__builtin_expect(lkCheck,0)){
     // Test mode: every line the kernel took is put through the line code's own functions and compared.
     for(int i=0;i<8;++i){
      const bool kok=(o.ok>>i)&1,ksl=(o.slow>>i)&1;
      if(!kok&&!ksl){if(kp[i]!=was[i]||o.idx[i]){cerr<<"LINKCHECK FAIL dead lane changed\n";abort();}continue;}
      string_view f[2];const char* next;LinkDense r{};
      const bool ok=link_split(was[i],end,f,next);
      if(ok)link_dense(f[0].data(),f[0].size(),f[1].data(),f[1].size(),r);
      const bool simple=ok&&r.da&&r.db&&r.ia==r.ib&&r.aa<8&&r.ab<8;
      if(kok==ksl||(kok&&(!simple||r.ia!=o.idx[i]||r.aa!=o.aa[i]||r.ab!=o.ab[i]||next!=kp[i]||(((o.un>>i)&1)!=(r.aa!=r.ab))))||(!kok&&(kp[i]!=was[i]||o.idx[i]||((o.un>>i)&1)))){
       cerr<<"LINKCHECK FAIL line '"<<string_view(was[i],(size_t)min<ptrdiff_t>(next-was[i],60))<<"' kernel ok "<<kok<<" slow "<<ksl<<" idx "<<o.idx[i]<<" aa "<<o.aa[i]<<" ab "<<o.ab[i]<<" line code simple "<<simple<<" idx "<<r.ia<<" aa "<<r.aa<<" ab "<<r.ab<<"\n";abort();}
      if(ksl&&simple)++out.missed;
      const bool kun=(o.un>>i)&1;
      if(o.bk[i]!=(kun?((o.idx[i]>>vlk::LK_BLOCK_BITS)&lkc.bmask):lkc.btrash)||(kun&&(o.rec[i]>>32||o.rec[i]>>5!=o.idx[i]||vlk::LK_PAIR_LO[o.rec[i]&31]!=min(o.aa[i],o.ab[i])||vlk::LK_PAIR_HI[o.rec[i]&31]!=max(o.aa[i],o.ab[i])))){
       cerr<<"LINKCHECK FAIL record "<<o.rec[i]<<" bucket "<<o.bk[i]<<" idx "<<o.idx[i]<<" aa "<<o.aa[i]<<" ab "<<o.ab[i]<<"\n";abort();}
     }
    }
    nk+=POP8[o.ok];
    if(__builtin_expect(o.slow,0)){
     for(unsigned m=o.slow;m;m&=m-1){const int i=__builtin_ctz(m);++nslow;
      const char* const next=one_line(kp[i]);
      if(next<klim[i])kp[i]=next;else{tail[i]=next;kp[i]=klim[i]=ck.base;}} // the lane ends here: its pointer may be too close to the file's end to be read
    }
    if(lkCas){if(prev)unions(*prev);prev=&o;cur^=1;}
    else{
     if(__builtin_expect(room==0,0))room=refill();
     --room;
     uint32_t** const bp=out.bp;
     for(int i=0;i<8;++i){const uint64_t r=o.bk[i];uint32_t* const q=bp[r];*q=(uint32_t)o.rec[i];bp[r]=q+1;} // a lane without a union writes to the spare place
     bp[lkR]=out.trash;
    }
   }
   if(prev)unions(*prev);
   for(int j=0;j<8;++j){
    maxIdx=max(maxIdx,(uint32_t)max(ob[0].mx[j],ob[1].mx[j]));
    for(const char* p=tail[j]?tail[j]:kp[j];p<s[j+1];++out.ntail)p=one_line(p);
   }
   apply();out.maxIdx=maxIdx;out.nk+=nk;out.nslow+=nslow;
  }
  // Second step of the lane loop: every bucket's records, from all threads' lists, applied by one thread.
  if(lkOn&&!lkCas){
   #pragma omp for schedule(dynamic,1) nowait
   for(int r=0;r<lkR;++r){
    const double t0=lkStats?thr_cpu_s():0.0;
    uint32_t* const words=(uint32_t*)denseComp; // plain loads and stores: this thread alone changes bucket r's words now
    const auto run_block=[&](const uint32_t* b,const uint32_t* e){if(lkRef)vlk::lk_union_ref(words,b,e);else vlk::lk_union_v5(words,b,e);};
    for(LinkOut& o:lout){
     for(const auto& bl:o.blocks[r])run_block(bl.first,bl.second);
     if(o.bstart[r])run_block(o.bstart[r],o.bp[r]);
    }
    if(lkStats)lout[omp_get_thread_num()].cpu2+=thr_cpu_s()-t0;
   }
  }
  }
 }
 __fine("link-pass");
 // Test only, SS_LINKS_DUMP=file: what the first pass left behind - every non-zero alias word, every complex index and the
 // number of lines kept for the ID table - as text (compared between SS_LINKS=old and the lane loop).
 if(const char* v=getenv("SS_LINKS_DUMP"))if(denseUsers){ofstream o(v,ios::binary);
  uint64_t gen=0;uint32_t mx=0;for(const LinkOut& l:lout){gen+=l.gen.size();mx=max(mx,l.maxIdx);}
  o<<"gen "<<gen<<" maxIdx "<<mx<<"\n";
  for(uint64_t i=0;i<dense.limI;++i){const uint32_t w=denseComp[i].load(memory_order_relaxed);if(w)o<<"w "<<i<<' '<<w<<'\n';if(dense_complex(i))o<<"c "<<i<<'\n';}}
 if(getenv("SS_STATS")&&denseUsers){uint64_t nk=0,nslow=0,ntail=0,missed=0;for(const LinkOut& o:lout){nk+=o.nk;nslow+=o.nslow;ntail+=o.ntail;missed+=o.missed;}
  double cpu=0,cpu2=0;for(const LinkOut& o:lout){cpu+=o.cpu;cpu2+=o.cpu2;}
  cerr<<"STATS links: kernel lines "<<nk<<" refused "<<nslow<<" (simple among them, check modes only: "<<missed<<") file tails "<<ntail<<" thread cpu ms "<<cpu*1e3<<" + bucket unions "<<cpu2*1e3<<"\n";}
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
 // SS_LINKS on the general route (no dense users: every ID is an opaque text): unless SS_LINKS=old the link lines go
 // through the "gen" form of the stages below - the usual line (two fields of up to 11 bytes, bare or quoted) is cut
 // and packed from two 16-byte loads, new IDs note their table slot so that FLATTEN rewrites it without hashing, and the
 // union-find entries need no first pass (see LkUF). Same table, same numbers' meaning, same names; only the cost differs.
 const bool lkGen=!denseUsers&&lkm!="old";
 const uint64_t maxLinked=2*linkLines+((idBlocks||lkGen)?(uint64_t)T*KeyTable::ID_BLOCK:0);
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
 atomic<uint32_t>* const parent=lkGen?nullptr:(atomic<uint32_t>*)map_huge((maxLinked+1)*sizeof(uint32_t)); // stays mapped: the process ends with _exit
 auto find_root=[&](uint32_t u)->uint32_t{
  for(;;){
   uint32_t p=parent[u].load(memory_order_relaxed);if(p==u)return u;
   uint32_t gp=parent[p].load(memory_order_relaxed);if(gp==p)return p;
   parent[u].compare_exchange_weak(p,gp); // path halving; losing the race is harmless
   u=gp;
  }
 };
 // best[r]: the ID with the byte-wise smallest name seen so far in root r's group.
 atomic<uint32_t>* const best=lkGen?nullptr:(atomic<uint32_t>*)map_huge((maxLinked+1)*sizeof(uint32_t));
 atomic<size_t> nextLink{0},nextReplay{0};uint64_t idHoles=0;
 size_t maxGenList=0;for(const LinkOut& o:lout)maxGenList=max(maxGenList,o.gen.size());
 // The "gen" form's union-find: one 8-byte entry per ID number, both fields "number + 1, 0 = myself", so the zero pages
 // of a fresh mapping already say "every ID is its own group and its own best name": no pass that writes 2 entries per
 // link line before the first line is read, whichever way an ID got its number (block, single number of a long key).
 //   p: parent + 1 (a parent's number is smaller than its child's, so the forest cannot form a cycle), 0 = root
 //   b: of a root, the member with the byte-wise smallest name so far + 1 (FLATTEN only), 0 = the root itself
 // lkSlot[id]: where the value of an ID added by the link loop lives - table slot + 1, or 0x80000000 | index of its
 // long entry (0: added some other way, or out of range; FLATTEN then looks the key up).
 struct LkUF{atomic<uint32_t> p,b;};static_assert(sizeof(LkUF)==8);
 LkUF* const uf=lkGen?(LkUF*)map_huge(((size_t)maxIds+33)*sizeof(LkUF)):nullptr;
 uint32_t* const lkSlot=lkGen?(uint32_t*)map_huge(((size_t)maxIds+1)*sizeof(uint32_t)):nullptr;
 const auto uf_root=[uf](uint32_t u)__attribute__((always_inline))->uint32_t{
  for(;;){
   uint32_t p=uf[u].p.load(memory_order_relaxed);if(!p)return u;
   const uint32_t gp=uf[p-1].p.load(memory_order_relaxed);if(!gp)return p-1;
   uf[u].p.compare_exchange_weak(p,gp); // path halving (gp - 1 < p - 1 < u); losing the race is harmless
   u=gp-1;
  }
 };
 uint64_t lkFast=0,lkSlowN=0,lkLongN=0;double lkCpu=0;
 __mark("LINKSETUP");
 if(lkGen){
  static const bool gStats=getenv("SS_STATS")!=nullptr;
  // SS_LINKS=base: the SSE2 fill also where the CPU has AVX-512 (what runs without it, under SS_ISA=sse2 and in valgrind).
  const bool fill5=lkg::has_v5()&&lkm!="base"&&!(getenv("SS_ISA")&&string_view(getenv("SS_ISA"))=="sse2");
  const char* (*const fill)(lkg::Batch&,const char*,const char*,const char*,const KeyTable::Slot*,size_t)=fill5?lkg::fill_v5:lkg::fill_base;
  #pragma omp parallel num_threads(T) reduction(+:idHoles,lkFast,lkSlowN,lkLongN,lkCpu)
  {
   const double cpu0=gStats?thr_cpu_s():0.0;
   // Lines are handled in batches of lkg::LB (2 IDs per line) that move through five steps, one step per turn, so
   // that every random access was asked for a whole turn (the work of all steps on 16 lines) before it is made:
   //   turn t    cut and pack the lines, hash the keys, ask for the table slots               (lkg::fill_*)
   //   t + 1     ask for the entries of the long keys whose home slot holds a long key
   //   t + 2     the number of every ID (new IDs get one); ask for the union-find entries
   //   t + 3     ask for the entries of their parents
   //   t + 4     the unions
   // The batches live in a ring of eight; a turn's work on an empty batch is nothing, which is also how the last
   // batches leave (four turns without lines). Order does not matter to the result: a number is a name for a key,
   // and the groups are a function of the set of unions.
   static constexpr uint64_t RN=8;
   lkg::Batch ring[RN];
   KeyTable::Slot* const slots=tab.slots;PName* const names=tab.newNames;const size_t tmask=tab.mask;
   LkUF* const ufp=uf;uint32_t* const note=lkSlot;
   const size_t noteLim=getenv("SS_TEST_LKNOTE")?0:0x7FFFFFFFu; // test switch: no notes, so FLATTEN looks every key up (its path for a note out of range)
   KeyTable::IdBlock blk;
   // A number for a new ID, from this thread's block (cannot run out: the table holds every ID the link lines can name).
   const auto new_id=[&]()__attribute__((always_inline))->uint32_t{
    if(__builtin_expect(blk.next==blk.end,0)){
     const uint32_t b=tab.nextId.fetch_add(KeyTable::ID_BLOCK,memory_order_relaxed);
     if(b>=tab.idLimit){cerr<<"ERROR link ID table full\n";_exit(1);} // (the sizes above rule it out)
     blk.next=b;blk.end=tab.idLimit-b<KeyTable::ID_BLOCK?tab.idLimit:b+KeyTable::ID_BLOCK;
    }
    return blk.next++;
   };
   // KeyTable::find_or_add_packed_block, plus the note of the slot a new ID went into.
   const auto id_packed=[&](uint64_t k0,uint32_t k1,size_t j)__attribute__((always_inline))->uint32_t{
    const auto R=memory_order_relaxed;
    for(;;){
     KeyTable::Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
     if(c==k1){if(e.k0.load(R)==k0)return e.v.load(R);}
     else if(c==0){
      if(blk.next==blk.end){const uint32_t id0=new_id();blk.next=id0;} // refill before claiming the slot
      uint32_t expect=0;
      if(!e.k1.compare_exchange_strong(expect,KeyTable::BUSY,memory_order_acquire))continue;
      const uint32_t id=blk.next++;
      names[id].set_packed(k0,k1);note[id]=j<noteLim?(uint32_t)j+1:0;
      e.k0.store(k0,R);e.v.store(tab.idBase+id,R);e.k1.store(k1,memory_order_release);
      return tab.idBase+id;
     }
     else if(c==KeyTable::BUSY){__builtin_ia32_pause();continue;}
     j=(j+1)&tmask;
    }
   };
   // KeyTable::find_or_add_long for a key the fill has already made (B, entry q), with the number from the block and
   // the note of the entry a new ID went into.
   const auto id_long=[&](const lkg::Batch& B,int q)__attribute__((noinline))->uint32_t{
    const auto R=memory_order_relaxed;
    KeyTable::Long key;key.a=B.a[q];key.b=B.b[q];key.kind=B.kind[q];key.v=0;const uint64_t h=B.h[q];
    for(size_t j=B.j[q];;){
     KeyTable::Slot& e=slots[j];uint32_t c=e.k1.load(memory_order_acquire);
     if(c==KeyTable::LONGK){if(e.k0.load(R)==h){const KeyTable::Long& l=tab.longs.d[e.v.load(R)];if(KeyTable::same_long(l,key))return l.v;}}
     else if(c==0){
      if(blk.next==blk.end){const uint32_t id0=new_id();blk.next=id0;}
      uint32_t expect=0;
      if(!e.k1.compare_exchange_strong(expect,KeyTable::BUSY,memory_order_acquire))continue;
      const uint32_t id=blk.next++;
      const size_t li=tab.longs.n.fetch_add(1,R); // below longs.room: at most idLimit IDs are added in this mode
      names[id].set(string_view(B.s[q],B.len[q]));key.v=tab.idBase+id;tab.longs.d[li]=key;
      note[id]=li<noteLim?0x80000000u|(uint32_t)li:0;
      e.k0.store(h,R);e.v.store((uint32_t)li,R);e.k1.store(KeyTable::LONGK,memory_order_release);
      return tab.idBase+id;
     }
     else if(c==KeyTable::BUSY){__builtin_ia32_pause();continue;}
     j=(j+1)&tmask;
    }
   };
   const char* p=nullptr;const char* lim=nullptr;const char* end=nullptr;bool more=true;int idle=0;
   for(uint64_t t=0;;++t){
    {lkg::Batch& B=ring[t%RN];B.n=0;B.nlong=0;
     // The next piece of a link file when the current one is done: every data line that begins in [from,to), as in
     // for_lines; exactly two non-empty fields or the line is skipped.
     while(more&&p>=lim){
      const size_t c=nextLink.fetch_add(1,memory_order_relaxed);if(c>=linkChunks.size()){more=false;break;}
      const Chunk& ck=linkChunks[c];
      if(ck.from>=ck.len)continue;
      const char* nl=(const char*)memchr(ck.from?ck.base+ck.from-1:ck.base,'\n',ck.from?ck.len-(ck.from-1):ck.len);
      if(!nl)continue;
      end=ck.base+ck.len;lim=ck.base+min(ck.to,ck.len);p=nl+1;
     }
     if(more)p=fill(B,p,lim,end,slots,tmask);
     else if(++idle>4)break;}
    {const lkg::Batch& B=ring[(t+RN-1)%RN];
     for(int i=0;i<B.nlong;++i){const KeyTable::Slot& e=slots[B.j[B.lq[i]]];if(e.k1.load(memory_order_acquire)==KeyTable::LONGK)__builtin_prefetch(&tab.longs.d[e.v.load(memory_order_relaxed)],0,1);}}
    {lkg::Batch& B=ring[(t+RN-2)%RN];const int n=B.n;
     for(int q=0;q<n;++q){
      const uint32_t id=__builtin_expect(B.k1[q]!=0,1)?id_packed(B.a[q],B.k1[q],B.j[q]):id_long(B,q);
      B.id[q]=id;__builtin_prefetch(&ufp[id],1,1);
     }}
    {const lkg::Batch& B=ring[(t+RN-3)%RN];const int n=B.n;
     for(int q=0;q<n;++q){const uint32_t pp=ufp[B.id[q]].p.load(memory_order_relaxed);if(pp)__builtin_prefetch(&ufp[pp-1],1,1);}}
    {const lkg::Batch& B=ring[(t+RN-4)%RN];const int n=B.n;
     for(int q=0;q<n;q+=2){
      uint32_t a=B.id[q],b=B.id[q+1];
      for(;;){
       a=uf_root(a);b=uf_root(b);if(a==b)break;
       if(a<b)swap(a,b);
       uint32_t expect=0;if(ufp[a].p.compare_exchange_strong(expect,b+1))break; // a was still a root: now under the smaller root b
      }
     }}
   }
   idHoles+=blk.end-blk.next;for(const lkg::Batch& B:ring){lkFast+=B.fast;lkSlowN+=B.slow;lkLongN+=B.longs;}
   if(gStats)lkCpu+=thr_cpu_s()-cpu0;
  }
  if(gStats)cerr<<"STATS links gen: fill "<<(fill5?"v5":"base")<<" lines cut by the fill "<<lkFast<<" through link_split "<<lkSlowN<<" (long keys: "<<lkLongN<<") thread cpu ms "<<lkCpu*1e3<<"\n";
 }
 else{
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
 }
 // nlinked: one past the highest linked ID number (with ID blocks a few numbers below it were never used:
 // they stay empty one-member groups that nothing refers to).
 const uint32_t nlinked=min(tab.nextId.load(),tab.idLimit);
 // Room for maxNew64 more IDs from events and clicks (also what SS_TEST_MAXNEW limits).
 tab.idLimit=(uint32_t)min<uint64_t>(tab.idLimit,(uint64_t)nlinked+maxNew64);
 __mark("LINKREAD");
 // Flatten: every linked ID that is not its group's root offers its name to the root (the byte-wise
 // smallest wins) and has its table entry pointed at the root, which is the person number.
 if(lkGen){
  // The "gen" form: the same three steps per batch - parents asked for; roots found, the root's best name and the
  // ID's own table slot asked for; the name offered and the slot rewritten - with the slot taken from lkSlot (no
  // packing, hashing or probing; an ID without a note is looked up as before) and parent + best in one entry.
  PName* const names=tab.newNames;KeyTable::Slot* const slots=tab.slots;const size_t tmask=tab.mask;
  KeyTable::Long* const longs=tab.longs.d;atomic<bool> unnoted{false};
  #pragma omp parallel num_threads(T)
  {
   // Each thread takes one stretch of the numbers, 32 at a time, the three steps two turns apart (a ring of eight
   // lists), for the same reason as in the link loop: what a step reads was asked for two turns of work earlier.
   static constexpr uint32_t FB=32;static constexpr uint64_t FR=8;
   struct Fix{uint32_t i,r;};Fix fix[FR][FB];int nf[FR];for(uint64_t k=0;k<FR;++k)nf[k]=0;
   const uint64_t nth=(uint64_t)omp_get_num_threads(),tid=(uint64_t)omp_get_thread_num();
   const uint32_t hi=(uint32_t)((uint64_t)nlinked*(tid+1)/nth);uint32_t b0=(uint32_t)((uint64_t)nlinked*tid/nth);
   int idle=0;
   for(uint64_t t=0;;++t){
    {Fix* const fx=fix[t%FR];int n=0;
     if(b0<hi){const uint32_t b1=hi-b0<FB?hi:b0+FB;
      for(uint32_t i=b0;i<b1;++i){
       const uint32_t p=uf[i].p.load(memory_order_relaxed);if(!p)continue;
       fx[n++]={i,p-1};__builtin_prefetch(&uf[p-1],1,1);
      }
      b0=b1;}
     else if(++idle>4){nf[t%FR]=0;break;}
     nf[t%FR]=n;}
    {Fix* const fx=fix[(t+FR-2)%FR];const int n=nf[(t+FR-2)%FR];
     for(int q=0;q<n;++q){Fix& f=fx[q];
      const uint32_t r=uf_root(f.r);f.r=r;
      const uint32_t bb=uf[r].b.load(memory_order_relaxed);__builtin_prefetch(&names[bb?bb-1:r],0,1);
      const uint32_t sl=lkSlot[f.i];
      if(sl&0x80000000u)__builtin_prefetch(&longs[sl&0x7FFFFFFFu],1,1);else if(sl)__builtin_prefetch(&slots[sl-1],1,1);
     }}
    {const Fix* const fx=fix[(t+FR-4)%FR];const int n=nf[(t+FR-4)%FR];
     for(int q=0;q<n;++q){const Fix& f=fx[q];
      uint32_t cur=uf[f.r].b.load(memory_order_relaxed);
      while(name_less(names[f.i],names[cur?cur-1:f.r])&&!uf[f.r].b.compare_exchange_weak(cur,f.i+1)){}
      const uint32_t sl=lkSlot[f.i];
      if(sl&0x80000000u)longs[sl&0x7FFFFFFFu].v=f.r; // a long key: its value is in its entry
      else if(__builtin_expect(sl!=0,1))slots[sl-1].v.store(f.r,memory_order_relaxed);
      else{uint64_t k0;uint32_t k1;
       if(KeyTable::pack(names[f.i].view(),k0,k1))tab.set_packed(k0,k1,KeyTable::hash_packed(k0,k1)&tmask,f.r);
       else unnoted.store(true,memory_order_relaxed);} // a long key without a note: the pass over all long entries below
     }}
   }
   // A root's own name is no longer needed once every member has been compared, and no other person reads it.
   #pragma omp barrier
   #pragma omp for schedule(static)
   for(uint32_t i=0;i<nlinked;++i){ // (b is only ever set in a root's entry; the array has 32 spare entries for the look ahead)
    const uint32_t bn=uf[i+16].b.load(memory_order_relaxed);if(bn)__builtin_prefetch(&names[bn-1],0,1);
    const uint32_t bi=uf[i].b.load(memory_order_relaxed);if(bi)names[i]=names[bi-1];
   }
  }
  if(unnoted.load())for(auto& l:tab.longs)if(l.v<nlinked)l.v=uf_root(l.v); // (a value that is a root already stays)
 }
 else{
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
 __fine("catalog-join");
 if(catalogBg){catalogThread.join();if(catalogErr)rethrow_exception(catalogErr);}else load_catalog();
 cerr<<"click path: campaign lookup "<<(campCheck?"check":campTable.look.lim?"table":"hash")<<" ("<<campTable.keys<<" keys, "<<campTable.t.size()<<" slots)\n";
 __fine("catalog-ready");

 // ---- Parse every event and click line in parallel, bucketing by ID hash. ----
 const size_t PE=pow2_parts(eventBytes/70),PC=pow2_parts(clickBytes/40);
 const size_t UP=pow2_parts(eventBytes/70); // person buckets for the last stage
 // SS_ISA=sse2 forces the portable splitter (used by tests to check both paths give the same tables).
 // SS_ISA=avx512z / avx512y pick an AVX-512 mask builder (see the line loops) when the CPU has AVX-512BW+VL;
 // without it they fall back to avx2. The default is avx2.
 const bool hasV3=__builtin_cpu_supports("avx2")&&__builtin_cpu_supports("bmi")&&__builtin_cpu_supports("bmi2")&&__builtin_cpu_supports("popcnt")&&__builtin_cpu_supports("lzcnt");
 const bool hasV5=hasV3&&__builtin_cpu_supports("avx512f")&&__builtin_cpu_supports("avx512bw")&&__builtin_cpu_supports("avx512vl");
 const bool hasE8=hasV5&&__builtin_cpu_supports("avx512dq")&&__builtin_cpu_supports("avx512cd"); // what vert::ev8_v5 needs beyond the AVX-512 loops
 enum{ISA_SSE2,ISA_AVX2,ISA_AVX512Z,ISA_AVX512Y};
 int isaUse=hasV3?ISA_AVX2:ISA_SSE2;
 if(const char* isa=getenv("SS_ISA")){const string_view w(isa);
  if(w=="sse2")isaUse=ISA_SSE2;
  else if(w=="avx512z"&&hasV5)isaUse=ISA_AVX512Z;
  else if(w=="avx512y"&&hasV5)isaUse=ISA_AVX512Y;}
 // The vector loops locate line starts ahead of cutting the lines (see SS_EVENT_LOOP_IX): measured 5% faster
 // in the parse stage on the scoring machine. SS_LINES=plain switches back to the loop without the index.
 const bool lineIndex=isaUse!=ISA_SSE2&&!(getenv("SS_LINES")&&string_view(getenv("SS_LINES"))=="plain");
 // The event loop with the AVX-512 kernel (vert::ev8_v5) is the default for dense-record event files on a CPU that
 // has it: measured on the scoring machine at less than half the time per row of the two-phase loop. With no
 // SS_ISA the event files then use the avx512z masks (the kernel uses 512-bit registers anyway) while the click
 // files stay on the avx2 loop; with SS_ISA=avx512z|avx512y everything follows that choice. Any SS_LINES value
 // switches the default off (SS_LINES=ev8 asks for the loop explicitly, also on avx2 with the scalar kernel).
 // SS_EVLEAD=2 / SS_CLLEAD=2: the ev8 / cl8 loop keeps one more group under way (a decoded group waits a turn before
 // it is finished), so a slot is asked for 16 to 24 lines before its store instead of 8 to 16. SS_PF=w|t1|none: see optPfw.
 // All three are for measuring on the scoring machine; same output.
 const bool evLead2=getenv("SS_EVLEAD")&&string_view(getenv("SS_EVLEAD"))=="2";
 const bool clLead2=getenv("SS_CLLEAD")&&string_view(getenv("SS_CLLEAD"))=="2";
 if(const char* v=getenv("SS_PF")){const string_view w(v);optPfw=w=="w"?1:w=="t1"?2:w=="none"?3:0;}
 // SS_CLWIN=1: click lines of 64 bytes or more go to the general handler (as before GFAST); check: see click_long_check.
 if(const char* v=getenv("SS_CLWIN")){const string_view w(v);optClickWin2=w=="1"?0:w=="check"?2:1;}
 #define SS_LEAD_CALL(two_,fn_,args_) {if(two_)fn_<2>args_;else fn_<1>args_;}
 // SS_EV8=3: the ev8 loop runs kernel version 3 (gathers, date memo) instead of version 4. Same output.
 const bool ev8V3=getenv("SS_EV8")&&string_view(getenv("SS_EV8"))=="3";
 const bool ev8Auto=hasE8&&dparse.twoPhase&&!getenv("SS_LINES")&&isaUse!=ISA_SSE2&&!(getenv("SS_ISA")&&isaUse==ISA_AVX2);
 // The same for click files (vcl::cl8, SS_CLICK_LOOP_CL8; dense click records in slot form): the default on a CPU
 // with the kernel's extensions, with the avx512z masks when no SS_ISA is given. Any SS_CLICKS value other than cl8,
 // SS_ISA=avx2 or SS_DENSE_AHEAD (which asks for the ring of the old loop) keeps the click loop with the line index.
 // SS_CL8=1: the cl8 loop runs kernel version 1 (gathers, date memo) instead of version 2. Same output.
 const bool cl8V1=getenv("SS_CL8")&&string_view(getenv("SS_CL8"))=="1";
 // Lanes (SS_EVENT_LOOP_EV8L): wherever the ev8 loop runs kernel version 4, eight lanes walk eight pieces of a chunk and
 // there is no line index; the same for the cl8 loop with kernel version 2 (SS_CLICK_LOOP_CL8L). SS_LANES=0: the loops
 // with the index; SS_LANES=e / c: lanes for event / click files only. Same output. SS_LANES_PF=n / SS_LANES_PF2=n: see lanePf.
 const char* const lanesEnv=getenv("SS_LANES"); // 0 = neither, e = events only, c = clicks only
 if(const char* v=getenv("SS_LANES_PF")){const long long x=atoll(v);lanePf=x>0&&x<=(1<<20)?(unsigned)x:0;}
 if(const char* v=getenv("SS_LANES_PF2")){const long long x=atoll(v);lanePf2=x>0&&x<=(1<<20)?(unsigned)x:0;}
 const bool lanes=!(lanesEnv&&(string_view(lanesEnv)=="0"||string_view(lanesEnv)=="c")),lanesC=!(lanesEnv&&(string_view(lanesEnv)=="0"||string_view(lanesEnv)=="e"));
 // The general route's event loop (GFAST.md packages A + B): the ev8g kernel in lanes (SS_EVENT_LOOP_G8L) wherever the CPU
 // has the kernel's extensions and neither SS_LINES nor SS_ISA=avx2|sse2 asks for another loop. SS_GEV=old: the loop
 // with the line index as before; ref: the scalar twin of the kernel in the same loop (any AVX2 CPU; what callgrind and
 // the sanitizers can run); check / checkref: every row the kernel takes is also given to the general handler and the
 // two records are compared (abort on a difference). Same output for all.
 const string_view gevW(getenv("SS_GEV")?getenv("SS_GEV"):"");
 const bool gevRef=hasV3&&isaUse!=ISA_SSE2&&(gevW=="ref"||gevW=="checkref");
 const bool gevVec=!gevRef&&hasE8&&gevW!="old"&&!getenv("SS_LINES")&&isaUse!=ISA_SSE2&&!(getenv("SS_ISA")&&isaUse==ISA_AVX2);
 gevCheck=gevW=="check"||gevW=="checkref";
 gevLongText=getenv("SS_GEV_LONG")&&string_view(getenv("SS_GEV_LONG"))=="text";
 if(const char* v=getenv("SS_ABL")){const string_view w(v);gevAbl=w=="nouser"?1:w=="norec"?2:w=="nokern"?3:0;}
 const bool gevUse=(gevRef||gevVec)&&!dparse.events&&!dparse.users;
 // SS_GCOMMIT=stream (needs the loop above for every event file): the rows go to GRec / GRecL streams of 2^gbits
 // buckets per thread instead of PRec buckets. gbits: the smallest with estimated rows / buckets <= 6,000, 6..12
 // (4,096 fill lines of 64 bytes per thread at most); SS_GBITS=n (1..16) sets it. SS_GSPF=0|1: ask for a row's fill line
 // and bucket state a group ahead (default: from 1,024 buckets). Until a stage reads the streams, each thread's
 // streams are turned back into its PRec buckets after PARSE (gbridge, stage mark GBRIDGE): same output.
 dparse.gstream=gevUse&&getenv("SS_GCOMMIT")&&string_view(getenv("SS_GCOMMIT"))=="stream";
 if(dparse.gstream){unsigned gb=6;while(gb<12&&(estEvents>>gb)>6000)++gb;
  if(const char* v=getenv("SS_GBITS")){const long long x=atoll(v);gb=(unsigned)(x<1?1:x>16?16:x);}
  dparse.gbits=gb;dparse.gspf=gb>=10;if(const char* v=getenv("SS_GSPF"))dparse.gspf=atoi(v)!=0;
  if(const char* v=getenv("SS_GNOTE"))dparse.gnote=atoi(v)!=0;} // SS_GNOTE=0: every record through StreamStore::put (a branch per row)
 const bool cl8Auto=hasE8&&dparse.clicks&&!dparse.bucket&&!getenv("SS_CLICKS")&&!getenv("SS_DENSE_AHEAD")&&isaUse!=ISA_SSE2&&!(getenv("SS_ISA")&&isaUse==ISA_AVX2);
 // The click loop of the general route (opaque IDs: no dense click records, no dense users) is the lane loop with
 // kernel cl8g (SS_CLICK_LOOP_CLG) on a CPU with the kernel's extensions; SS_CLG=0, any SS_CLICKS or SS_LINES value,
 // SS_LANES=0|e or SS_ISA=avx2|sse2 keep the loop with the line index. SS_CLG=ref|refcheck: the scalar twin on any CPU with AVX2.
 if(const char* v=getenv("SS_CLG")){const string_view w(v);optClg=w=="0"?0:w=="check"?2:w=="ref"?3:w=="refcheck"?4:1;}
 const bool clgRef=optClg>=3&&isaUse!=ISA_SSE2&&!dparse.clicks&&!dparse.users;
 const bool clgUse=clgRef||(optClg&&hasE8&&!dparse.clicks&&!dparse.users&&!getenv("SS_CLICKS")&&!getenv("SS_LINES")&&lanesC&&isaUse!=ISA_SSE2&&!(getenv("SS_ISA")&&isaUse==ISA_AVX2));
 cerr<<"splitter: "<<(isaUse==ISA_AVX512Z?"avx512z":isaUse==ISA_AVX512Y?"avx512y":isaUse==ISA_AVX2?"avx2":"sse2")<<(lineIndex?"+index":"")<<(clgUse?(clgRef?" clicks: cl8g kernel (scalar twin) in lanes":" clicks: cl8g kernel in lanes"):"")<<(clgUse&&(optClg==2||optClg==4)?" (check)":"")
  <<(ev8Auto?(ev8V3?" events: ev8 kernel v3":lanes?" events: ev8 kernel v4 in lanes":" events: ev8 kernel v4"):"")<<(cl8Auto?(cl8V1?" clicks: cl8 kernel v1":lanesC?" clicks: cl8 kernel v2 in lanes":" clicks: cl8 kernel v2"):"")<<(gevUse?(gevRef?" general events: ev8g scalar twin in lanes":" general events: ev8g kernel in lanes"):"")<<(gevUse&&gevCheck?" (checked)":"")<<(dparse.gstream?" -> GRec streams":"")<<(evLead2?" evlead 2":"")<<(clLead2?" cllead 2":"")<<(optPfw==1?" pf w":optPfw==2?" pf t1":optPfw==3?" pf none":"")<<"\n";
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
 // Stream mode: buckets of every thread's store, and the chunk size (halved while the first chunks of all buckets
 // would be more than half of a thread's expected rows, or than 8 MB if that is more).
 const size_t streamNB=dparse.stream?(size_t)(dense.limE>>dparse.bsh)+1:0;
 unsigned streamCS=dparse.bch;
 // SS_TEST_STREAM_EST=n (test only): the stores are sized for n event rows instead of the estimate from the file size,
 // and their arenas grow in the smallest steps (the estimate can be far off: rows of other lengths, many copies).
 const bool streamEstTest=getenv("SS_TEST_STREAM_EST")!=nullptr;
 const uint64_t streamEst=streamEstTest?strtoull(getenv("SS_TEST_STREAM_EST"),nullptr,10):estEvents;
 if(dparse.stream){const uint64_t per=streamEst*sizeof(ERec)/(uint64_t)T;while(streamCS>6&&((uint64_t)streamNB<<streamCS)>max<uint64_t>(uint64_t(8)<<20,per/2))--streamCS;}
 __fine("parse-region");
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
   if(dparse.gstream){const size_t nb=size_t(1)<<dparse.gbits;const uint64_t per=estEvents/(uint64_t)T+1;unsigned cs=12;while(cs>6&&((uint64_t)nb<<cs)>max<uint64_t>(uint64_t(4)<<20,per*sizeof(GRec)/2))--cs;
    tl.gs.init(nb,cs,(size_t)(per*sizeof(GRec)*9/8));tl.gl.init(nb,cs,(size_t)(per*sizeof(GRecL)/6));}
   if(dparse.stream){tl.es.init(streamNB,streamCS,(size_t)(streamEst*sizeof(ERec)/(uint64_t)T*5/4));
    if(streamEstTest)tl.es.nextBytes=size_t(2)<<20;} // test: the arena grows 2 MB at a time
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
    else{tl.evEnd=max(tl.evEnd,(uint32_t)ve+1);
     if(dparse.stream)estream_put(tl.es,dparse.bsh,ve,k?k:ESLOT_INVALID,eslot_uct(user,cat,0),ingest,0);
     else eslot_commit(denseEv[ve],k?k:ESLOT_INVALID,eslot_uct(user,cat,tl.tid),ingest);}
    return;
   }
   uint64_t code=testSide?INGEST_SIDE:ingest_code(ingest);
   if(__builtin_expect(tl.gcap!=nullptr,0)){ThreadState::GCap& c=*tl.gcap;c.prec=make_prec(f[0].data(),h,code,k,cat,user);c.cls=gkey_classify(f[0].data(),f[0].size(),c.key,c.hi);c.ingest=ingest;c.has=true;return;} // SS_GEV=check
   if(dparse.gstream){gcommit_row(tl,dparse.gbits,f[0].data(),f[0].size(),k,user,cat,ingest);return;} // general route, SS_GCOMMIT=stream
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
   if(__builtin_expect(optPopulate==1,0)&&ck.from<ck.len)madvise((void*)(ck.base+ck.from),min(ck.to,ck.len)-ck.from,MADV_POPULATE_READ); // SS_POPULATE=ahead
   // Dense records (DREC loops) exist with the line index only; SS_LINES=plain then applies to the other kind of file.
   const bool drec=ck.click?dparse.clicks:dparse.events;
   const bool ev8Use=(dparse.ev8&&(isaUse==ISA_AVX2||hasE8))||(ev8Auto&&isaUse!=ISA_AVX2); // the avx2 loop has the scalar kernel; the AVX-512 loops need the vector one
   const bool cl8Use=(dparse.cl8&&(isaUse==ISA_AVX2||hasE8))||(cl8Auto&&isaUse!=ISA_AVX2); // as ev8Use
   if(ck.click&&!drec&&clgUse){if(clgRef)clicks_avx2_cg(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click);else clicks_avx512z_cg(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click);continue;} // the general route's click loop
   if(ck.click&&drec&&cl8Auto&&isaUse==ISA_AVX2){if(cl8V1)SS_LEAD_CALL(clLead2,clicks_avx512z_c8v1,(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click)) else if(lanesC)clicks_avx512z_c8l(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click); else SS_LEAD_CALL(clLead2,clicks_avx512z_c8,(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click)) continue;} // the default on an AVX-512 CPU
   if(!ck.click&&!drec&&gevUse){if(gevRef)events_avx2_g8l(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event);else events_avx512z_g8l(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event);continue;} // general route, events
   if(!ck.click&&drec&&ev8Auto&&isaUse==ISA_AVX2){if(ev8V3)SS_LEAD_CALL(evLead2,events_avx512z_e8v3,(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event)) else if(lanes)events_avx512z_e8l(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); else SS_LEAD_CALL(evLead2,events_avx512z_e8,(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event)) continue;} // the default on an AVX-512 CPU
   #define SS_RUN_LOOPS(ISA) \
    if(ck.click){if(drec&&cl8Use&&cl8V1)SS_LEAD_CALL(clLead2,clicks_##ISA##_c8v1,(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click)) \
     else if(drec&&cl8Use&&lanesC)clicks_##ISA##_c8l(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click); \
     else if(drec&&cl8Use)SS_LEAD_CALL(clLead2,clicks_##ISA##_c8,(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click)) \
     else if(drec)clicks_##ISA##_ix<true>(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click); \
     else if(lineIndex)clicks_##ISA##_ix<false>(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click); \
     else clicks_##ISA<false>(ck.base,ck.len,ck.from,ck.to,tl,tab,camps,PC,clickStart,on_click);} \
    else{if(drec&&ev8Use&&ev8V3)SS_LEAD_CALL(evLead2,events_##ISA##_e8v3,(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event)) \
     else if(drec&&ev8Use&&lanes)events_##ISA##_e8l(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); \
     else if(drec&&ev8Use)SS_LEAD_CALL(evLead2,events_##ISA##_e8,(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event)) \
     else if(drec&&dparse.batch)events_##ISA##_bt(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); \
     else if(drec&&dparse.twoPhase)events_##ISA##_2p(ck.base,ck.len,ck.from,ck.to,tl,tab,products,PE,on_event); \
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
  if(dparse.stream)tl.es.finish(); // partial fill lines out, non-temporal stores visible
  if(dparse.gstream){tl.gs.finish();tl.gl.finish();}
 }
 populateStop.store(true,memory_order_relaxed);
 __mark("PARSE");
 if(gevAbl){uint64_t x=0;for(int t=0;t<T;++t)x+=tls[t].gevAblSum;cerr<<"SS_ABL="<<getenv("SS_ABL")<<": TEST RUN, WRONG OUTPUT, stopping after the parse stage ("<<x<<")\n";cerr.flush();_exit(0);}
 if(dparse.gstream){
  if(getenv("SS_STATS")){const size_t nb=size_t(1)<<dparse.gbits;uint64_t ns=0,nl=0,big=0,chunks=0,bytes=0;
   for(size_t b=0;b<nb;++b){uint64_t n=0;for(int t=0;t<T;++t)n+=tls[t].gs.count(b)+tls[t].gl.count(b);big=max(big,n);}
   for(int t=0;t<T;++t){ns+=tls[t].gs.records();nl+=tls[t].gl.records();chunks+=tls[t].gs.chunks.size()+tls[t].gl.chunks.size();bytes+=tls[t].gs.arenaBytes+tls[t].gl.arenaBytes;}
   cerr<<"STATS gstream: buckets "<<nb<<" x threads "<<T<<", records "<<ns<<" short/other + "<<nl<<" long, mean per bucket "<<(ns+nl)/nb<<" largest "<<big<<", chunks "<<chunks<<" of "<<(size_t(1)<<tls[0].gs.cs)<<" bytes, arena "<<(bytes>>20)<<" MB"<<(dparse.gspf?", fill lines asked for a group ahead":"")<<"\n";}
  // Nothing reads the streams yet: back into the PRec buckets (each thread's own: LOCAL_USER numbers stay with their thread).
  #pragma omp parallel for num_threads(T) schedule(static,1)
  for(int t=0;t<T;++t){tlArena.stage=0;gbridge(tls[t],PE,dparse.gbits);
   vector<pair<char*,size_t>> maps;tls[t].gs.release(maps);tls[t].gl.release(maps);for(const auto& m:maps)munmap(m.first,m.second);}
  __mark("GBRIDGE");
 }
 // ---- SCATTER (SS_COMMIT=stream): the streams into the event slots, bucket by bucket ----
 // One thread owns a turn (2^bwin counters = one or more whole stream buckets), reads every thread's records of
 // those buckets in thread order and stores them by the rule of eslot_commit (erec_to_slot). The slots of a turn are
 // 512 KB by default, so the stores stay in the owner's second-level cache; no lock, no other writer. After it the
 // slot array is what the direct commit leaves (same winners; `thread` = the thread that parsed the winning copy),
 // and nothing downstream knows the difference.
 // Whoever replaces this step (BUCKETS.md stages B-D): bucket b = counters [b << dparse.bsh, (b+1) << dparse.bsh),
 // b < streamNB; stream_bucket_to_slots(tls,T,dparse.bsh,b,table,mask) loads it into any table (several threads at
 // once for different b, and again later for the same b: the streams are only read). Or record by record:
 //    for(int t=0;t<T;++t)                                // t = the thread that parsed the rows (the slot's `thread`)
 //     tls[t].es.for_segments(b,[&](const ERec* p,size_t n){...});   // n records at p (64-byte aligned), append order
 //   tls[t].es.count(b) = records of thread t in bucket b. Counter of a record = (b << dparse.bsh) | erec_lo(r).
 // The streams stay until release() (below; SS_BKEEP=1 keeps them to the end of the run).
 if(dparse.stream){
  const unsigned gsh=dparse.bwin-dparse.bsh;const size_t turns=(streamNB+(size_t(1)<<gsh)-1)>>gsh;
  uint64_t recs=0,chunksAll=0,bytesAll=0;
  #pragma omp parallel num_threads(T)
  {
   #pragma omp for schedule(dynamic,1) nowait
   for(size_t g=0;g<turns;++g)
    for(size_t b=g<<gsh;b<min(streamNB,(g+1)<<gsh);++b)stream_bucket_to_slots(tls,T,dparse.bsh,b,denseEv,~size_t(0));
  }
  if(getenv("SS_STATS")){for(int t=0;t<T;++t){recs+=tls[t].es.records();chunksAll+=tls[t].es.chunks.size();bytesAll+=tls[t].es.arenaBytes;}
   cerr<<"STATS stream: records "<<recs<<" buckets "<<streamNB<<" chunks "<<chunksAll<<" of "<<(size_t(1)<<streamCS)<<" bytes, arena bytes "<<bytesAll<<"\n";}
  // The streams are not needed again: a helper thread unmaps them in 16 MB steps (SS_BKEEP=1: they stay).
  if(!getenv("SS_BKEEP")){vector<pair<char*,size_t>> v;for(int t=0;t<T;++t)tls[t].es.release(v);
   thread([v=std::move(v)]{const size_t step=size_t(16)<<20;for(const auto& r:v)for(size_t at=0;at<r.second;at+=step)munmap(r.first+at,min(step,r.second-at));}).detach();}
  __mark("SCATTER");
 }
 // SS_SLOTSUM=1 (test only): three sums over the event slots as the parse stage (and SCATTER) left them, to compare
 // the direct commit with the stream commit on sets too large for the text dump. `all` covers the 32 bytes of every
 // slot in use with its counter (equal only if the same thread wrote each slot: one thread, or luck); `data` leaves
 // the thread out (equal between runs unless a row and its copy with the same ingest_ts were parsed by different
 // code: a raw user text in one, a user number in the other; which copy comes first depends on the threads);
 // `rows` = counter, k, ingest and the category of a valid row (must agree whatever the loop and the threads).
 if(getenv("SS_SLOTSUM")&&denseEv){
  auto mix=[](uint64_t x){x^=x>>33;x*=0xff51afd7ed558ccdull;x^=x>>29;x*=0xc4ceb9fe1a85ec53ull;x^=x>>32;return x;};
  uint64_t all=0,data=0,rows=0,used=0;const uint64_t e=denseEvEnd.load();
  for(uint64_t i=0;i<dense.limE;++i){const ESlot& r=denseEv[i];const uint64_t k=r.k.load(memory_order_relaxed);if(!k)continue;
   if(i>=e){cerr<<"SLOTSUM slot "<<i<<" in use beyond the end "<<e<<"\n";continue;}
   uint64_t w;memcpy(&w,(const char*)&r+offsetof(ESlot,user),8);
   const uint64_t a=mix(i+0x9E3779B97F4A7C15ull)^mix(k*3+1),b=mix((uint64_t)r.ingest+(a<<1));
   ++used;rows+=mix(a+b+(k>ESLOT_INVALID?mix(r.cat+7):0));data+=mix(a+b+mix(w&0x0000FFFFFFFFFFFFull)+mix(r.spare^0x5555));all+=mix(a+b+mix(w)+mix(r.spare^0x5555));}
  cerr<<"SLOTSUM end "<<e<<" used "<<used<<" rows "<<hex<<rows<<" data "<<data<<" all "<<all<<dec<<"\n";
 }
 if(getenv("SS_SMAPS")){ // measuring switch: how the kernel mapped things by the end of the parse stage (file PMD maps, anonymous huge pages)
  ifstream f("/proc/self/smaps_rollup");string l;
  while(getline(f,l))if(l.rfind("Rss:",0)==0||l.rfind("AnonHugePages:",0)==0||l.rfind("FilePmdMapped:",0)==0||l.rfind("ShmemPmdMapped:",0)==0||l.rfind("Anonymous:",0)==0)cerr<<"SMAPS "<<l<<"\n";
  __mark("SMAPS");
 }
 // ---- Raw users in the event slots (see DENSE_USER_RAW). The ID table still takes new IDs here. ----
 // Folded form (default): the event sweep of the DENSE stage runs here and reads the raw texts as it
 // goes; no slot is rewritten, so the slot array is read-only from here on and every later reader of
 // event slot users goes through RawCtx::code (rawSlots). Rows that are not a dense user's get their
 // table or LOCAL number here and wait in a per-thread list (k, user, cat, thread - the slot is not
 // needed again) until the person buckets exist. Only the piece that owns such a row asks the table.
 // SS_RESOLVE_RAW=pass: the separate pass that rewrites every raw slot, and the sweep after DEDUP, as
 // before (also when the slots are dumped for the parse checks, which expect resolved slots, and when
 // the index table cannot be mapped at its full size).
 // Event slots per piece of the sweep. SS_TEST_ECH: tests make the pieces tiny, so that runs cross piece ends all the time.
 static const size_t ECH=[]{const char* v=getenv("SS_TEST_ECH");const long long x=v?atoll(v):0;return x>0?(size_t)x:size_t(1)<<15;}();
 uint64_t rdSlotEv=0; // SS_STATS: valid event slots seen by the sweep
 vector<vector<ESmall>> sideRows(T); // folded form: the rows of table and LOCAL users, by sweeping thread
 vector<HugePool> sessPools(T);     // folded form: the session lists of each thread's runs
 bool rawSlots=false;
 if(dparse.raw&&denseEv){
  const size_t evEnd=denseEvEnd.load();
  {const char* v=getenv("SS_RESOLVE_RAW");rawSlots=!dumpDir&&!(v&&string_view(v)=="pass");}
  if(rawSlots){ // the index table by the index limit: the highest index is only known after this pass
   try{denseInfo=(DInfo*)map_huge(((size_t)dense.limI+2)*sizeof(DInfo));}catch(const bad_alloc&){rawSlots=false;}
  }
  if(rawSlots){
   for(int t=0;t<T;++t){sessPools[t].first=(((evEnd/4+4096)*8/(size_t)T)+(size_t(8)<<20));sessPools[t].next=max<size_t>(size_t(8)<<20,sessPools[t].first/4);}
   #pragma omp parallel num_threads(T) reduction(+:rdSlotEv)
   {
    const size_t t=(size_t)omp_get_thread_num();ThreadState& tl=tls[t];
    RawCtx rc;rc.v3=hasV3;SessArena arena;vector<ESmall>& side=sideRows[t];HugePool& pool=sessPools[t];
    const uint16_t tid=(uint16_t)tl.tid;
    uint64_t inRuns=0;
    #pragma omp for schedule(dynamic,1) nowait
    for(size_t lo=0;lo<evEnd;lo+=ECH)
     inRuns+=sweep_event_runs((const char*)denseEv,5,lo,min(evEnd,lo+ECH),evEnd,true,denseInfo,pool,arena,
      [&rc](const ESmall& sl){return rc.code(sl);},
      [](uint32_t){},
      [&](const ESmall& sl,uint32_t u){
       uint16_t th=sl.thread;
       if((sl.user&DENSE_USER_RAW)==DENSE_USER_RAW){ // a raw text that is not a dense user's: a number of this thread's making
        th=tid;
        if((u&DENSE_USER_RAW)==DENSE_USER_RAW){const uint32_t len=u&15u;const uint64_t raw=((const ESlot*)(const void*)&sl)->spare;
         u=raw_user_table(raw,len,tab,tl);rc.set_table(raw,len,u);}
       }
       side.push_back(ESmall{sl.k,u,sl.cat,th});
      });
    rdSlotEv+=inRuns;
    if(rc.dmax)dense_note_index((rc.dmax&~DENSE_USER)>>dn::ALIAS_BITS);
   }
  }
  else{
   // A thread's part of the slots is one contiguous range.
   #pragma omp parallel num_threads(T)
   {
    const size_t t=(size_t)omp_get_thread_num();ThreadState& tl=tls[t];uint32_t dmax=0;
    const size_t from=evEnd*t/(size_t)T,to=evEnd*(t+1)/(size_t)T;
    if(hasV3)resolve_raw_v3(denseEv,from,to,tab,tl,dmax);else resolve_raw_plain(denseEv,from,to,tab,tl,dmax);
    if(dmax)dense_note_index((dmax&~DENSE_USER)>>dn::ALIAS_BITS);
   }
  }
 }
 const uint32_t totalIds=tab.end_parallel();
 __mark("RESOLVE");
 if(campCheck)cerr<<"STATS campaign lookups checked against the KeyTable: "<<campTable.checked<<"\n";
 if(getenv("SS_STATS")){uint64_t cr=0,cs=0,lf=0;for(int t=0;t<T;++t){cr+=tls[t].clgRows;cs+=tls[t].clgSlow;lf+=tls[t].laneFixC;}
  if(cr)cerr<<"STATS cl8g click loop: lines "<<cr<<" through click_line_v3 "<<cs<<", line ends looked up "<<lf<<"\n";}
 if(getenv("SS_STATS")){uint64_t w2=0,cg=0;for(int t=0;t<T;++t){w2+=tls[t].clWin2;cg+=tls[t].clGeneral;}
  cerr<<"STATS click line code: three-window lines "<<w2<<", lines left to the general handler "<<cg<<(optClickWin2==2?" (SS_CLWIN=check)":optClickWin2==0?" (SS_CLWIN=1)":"")<<"\n";}
 if(getenv("SS_STATS")&&(denseEv||denseCl)){uint64_t w=0;for(int t=0;t<T;++t)w+=tls[t].slotWaits;
  cerr<<"STATS dense slots: event end "<<denseEvEnd.load()<<" click end "<<denseClEnd.load()<<" waits for a busy slot "<<w<<"\n";
  uint64_t cr=0,cs=0;for(int t=0;t<T;++t){cr+=tls[t].cl8Rows;cs+=tls[t].cl8Slow;}
  if(cr)cerr<<"STATS cl8 click loop: lines "<<cr<<" through click_line_v3 "<<cs<<"\n";
  uint64_t er=0,es=0;for(int t=0;t<T;++t){er+=tls[t].ev8Rows;es+=tls[t].ev8Slow;}
  if(er)cerr<<"STATS ev8 loop: rows "<<er<<" left to the line code "<<es<<"\n";
  uint64_t lsg=0,lfe=0,lfc=0;for(int t=0;t<T;++t){lsg+=tls[t].laneShort;lfe+=tls[t].laneFixE;lfc+=tls[t].laneFixC;}
  if(lsg|lfe|lfc)cerr<<"STATS lanes: groups with fewer than eight lanes at work "<<lsg<<", line ends looked up with memchr: event rows "<<lfe<<" click rows "<<lfc<<"\n";}
 if(getenv("SS_STATS")){uint64_t gr=0,gs=0,gc=0,gf=0,gsh=0;for(int t=0;t<T;++t){gr+=tls[t].gevRows;gs+=tls[t].gevSlow;gc+=tls[t].gevChecked;if(!denseEv){gf+=tls[t].laneFixE;gsh+=tls[t].laneShort;}}
  if(gr){char pc[32];snprintf(pc,sizeof pc,"%.3f",100.0*(double)gs/(double)gr);
   cerr<<"STATS gev loop: rows "<<gr<<" refused by the kernel (line code) "<<gs<<" = "<<pc<<"%, line ends looked up with memchr "<<gf<<", short groups "<<gsh<<", rows checked "<<gc<<"\n";}}
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
   for(const auto& l:tab.longs)o<<l.v<<'\t'<<hexstr(l.str())<<'\n';
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
  // The members of the root's group: the fields of the word that hold the root (bit 3a for alias a).
  const uint32_t x=(stored^COMP_IDENT)^(root*0x249249u);uint32_t members=~(x|(x>>1)|(x>>2))&0x249249u;
  uint64_t best=~0ull,bestV=0;
  do{
   const uint32_t a=(uint32_t)__builtin_ctz(members)/3u;members&=members-1;
   const uint64_t v=dn::value_of((idx<<dn::GEN_ALIAS_BITS)|a,dense.ku),key=order36(v);
   const uint64_t lt=0-(uint64_t)(key<best);bestV^=(bestV^v)&lt;best^=(best^key)&lt; // (masks: the compiler makes a branch of a ?: here)
  }while(members);
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
 const char* const evBase=nEvBuckets?(const char*)map_huge((size_t)(dense.limE+1)*sizeof(ESmall)):(const char*)denseEv;
 const unsigned evShift=nEvBuckets?4:5;
 const auto ev_at=[evBase,evShift](size_t c)->const ESmall&{return *(const ESmall*)(const void*)(evBase+(c<<evShift));};
 if(nClBuckets&&!denseCl)denseCl=(CSlot*)map_huge((size_t)(dense.limC+1)*sizeof(CSlot));
 const unsigned bshE=dparse.shE,bshC=dparse.shC;
 const auto db_size=[&](int t,size_t b)->size_t{return b<tls[t].db.size()?tls[t].db[b].size():0;};
 const auto dcb_size=[&](int t,size_t b)->size_t{return b<tls[t].dcb.size()?tls[t].dcb[b].size():0;};
 const bool slotMode=evBase||denseCl;
 if(slotMode&&!denseInfo)denseInfo=(DInfo*)map_huge(((size_t)denseMaxIdx.load()+2)*sizeof(DInfo));
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
  if(rawSlots)for(int t=0;t<T;++t)estA+=sideRows[t].size(); // the rows the event slots add to the first stretch: known exactly
  else if(denseEv){const size_t end=denseEvEnd.load(),step=max<size_t>(1,end/1024);size_t taken=0,tableRows=0;
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
 // Every general-lane row of a dense user (the rows that put the general-lane mark on a user index: a handful
 // per million) is also noted here: its index, its person, and where it lies in the general lists. The index
 // pass can then finish a marked index itself and take these rows along (see "marked indexes" there).
 // `person` is the row's person when it was passed on; a row that lost to an earlier copy afterwards holds its
 // bucket's tombstone number instead, which is never that person.
 // The notes are meant for a handful of rows. Data in which such rows are common (not the generator's) gets at
 // most genRefCap notes per thread; one thread reaching that switches the whole thing off (genRefOver), and
 // marked indexes are handed to the general lane as they always were. SS_TEST_REFCAP=n: tests make it small.
 struct GRef{uint32_t ix,person;void* at;bool click;};
 vector<vector<GRef>> genRefT((size_t)T);
 static const size_t genRefCap=[]{const char* v=getenv("SS_TEST_REFCAP");const long long x=v?atoll(v):(1ll<<18);return (size_t)(x<0?0:x);}();
 atomic<bool> genRefOver{false};
 // ---- Dedup each ID bucket on its own; survivors go to person buckets. ----
 uint64_t dedupCount=0,surviving=0,survivingClicks=0;
 uint64_t stEvN=0,stEvDisp=0,stEvMax=0,stEvFalse=0,stClN=0,stClDisp=0,stClMax=0,stClFalse=0; // SS_STATS (the two "False" counts are always kept: they cost nothing)
 #pragma omp parallel num_threads(T) reduction(+:dedupCount,surviving,survivingClicks,stEvN,stEvDisp,stEvFalse,stClN,stClDisp,stClFalse) reduction(max:stEvMax,stClMax)
 {
  const int tid=omp_get_thread_num();
  // With slots in use, a dense user whose row goes through the general lists is marked: the reader
  // then leaves all of that user index to the general lane.
  DInfo* const markInfo=slotMode?denseInfo:nullptr;
  vector<GRef>& myRefs=genRefT[(size_t)tid];
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
       size_t bkt;uint32_t markedIx=~0u;
       if((uint32_t)w&(LOCAL_USER|DENSE_USER)){
        const uint32_t ru=(uint32_t)w,pn=person_of(ru,t);
        w=(w&~0xffffffffull)|pn;
        if(ru&DENSE_USER){ // a dense person: second stretch
         bkt=bnA+((pn-bBase)>>bshB);
         if(markInfo){markedIx=(ru&~DENSE_USER)>>dn::ALIAS_BITS;markInfo[markedIx].general.store(1,memory_order_relaxed);}
        }
        else bkt=pn>>bshA;
       }
       else bkt=(uint32_t)w>>bshA; // a table number: first stretch
       EvList& L=myEv[bkt];Ev* const out=L.slot(pool);
       emitted[g1-1]=L.mark(out);
       __builtin_prefetch((const char*)out+ahead,1,3); // every time: testing for a new cache line would be an unpredictable branch
       out->k=rk-PR_KBIAS; // otherwise already in its final form
       memcpy((void*)&out->person,&w,8);
       if(__builtin_expect(markedIx!=~0u,0)){if(myRefs.size()<genRefCap)myRefs.push_back(GRef{markedIx,(uint32_t)w,(void*)out,false});else genRefOver.store(true,memory_order_relaxed);}
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
      uint32_t markedIx=~0u;
      if(r->user&DENSE_USER){bkt=bnA+((u-bBase)>>bshB);if(markInfo){markedIx=(r->user&~DENSE_USER)>>dn::ALIAS_BITS;markInfo[markedIx].general.store(1,memory_order_relaxed);}}
      else bkt=u>>bshA;
      Click* const out=myCl[bkt].slot(pool);
      __builtin_prefetch((const char*)out+ahead,1,3);
      out->key=r->key;out->id=r->id;out->person=u;out->idlen=r->idlen;
      if(__builtin_expect(markedIx!=~0u,0)){if(myRefs.size()<genRefCap)myRefs.push_back(GRef{markedIx,u,(void*)out,true});else genRefOver.store(true,memory_order_relaxed);}
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
 vector<GRef> genRefs; // all threads' notes, by user index
 const bool genRefsComplete=!genRefOver.load(); // every general-lane row of a dense user has its note
 if(genRefsComplete)for(const auto& v:genRefT)genRefs.insert(genRefs.end(),v.begin(),v.end());
 genRefT.clear();genRefT.shrink_to_fit();
 sort(genRefs.begin(),genRefs.end(),[](const GRef& a,const GRef& b){return a.ix!=b.ix?a.ix<b.ix:a.click!=b.click?a.click<b.click:(uintptr_t)a.at<(uintptr_t)b.at;});
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
 static constexpr size_t CCH=size_t(1)<<16,ICH=512; // slots / user indexes per piece of work (ECH: above)
 // Test switches: every n-th user index is treated as if its rows were spread out (it takes the
 // sweep of part 3) / as if it could not take the fast lane (its rows are handed over in part 2).
 static const uint32_t noTightN=[]{const char* v=getenv("SS_TEST_NOTIGHT");return v?(uint32_t)max(1,atoi(v)):0u;}();
 static const uint32_t noFastN=[]{const char* v=getenv("SS_TEST_NOFAST");return v?(uint32_t)max(1,atoi(v)):0u;}();
 uint64_t rdSlotCl=0,rdHandEv=0,rdHandCl=0,rdFastIdx=0,rdFastEv=0,rdFastCl=0,rdDeferIdx=0; // SS_STATS
 uint64_t rdMarkIdx=0,rdMarkPersons=0,rdMarkEv=0,rdMarkCl=0,rdMarkRows=0,rdMarkSorted=0,rdMarkPulled=0; // marked indexes finished by the index pass: how many, their events and clicks, general-lane rows among those, events that went through the full sort
 // Test switch: every n-th user index is treated as if one of its rows had gone through the general lane.
 static const uint32_t testMarkN=[]{const char* v=getenv("SS_TEST_MARK");return v?(uint32_t)max(1,atoi(v)):0u;}();
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
   // The sweep of one stretch of counters (events, then clicks). Session lists go to pieces of this
   // thread's pool, one block per run (sweep_event_runs). With rawSlots the event sweep has run already
   // (see RESOLVE) and only the rows it set aside are handed over here.
   SessArena arena;
   const auto sweep_events=[&](size_t lo,size_t hi,bool cross){
    uint64_t others=0;
    const uint64_t inRuns=sweep_event_runs(evBase,evShift,lo,hi,evEnd,cross,info,pool,arena,
     [](const ESmall& sl){return sl.user;},
     [&](uint32_t u){if(__builtin_expect(u>denseTop,0))bad_user();},
     [&](const ESmall& sl,uint32_t u){
      if(__builtin_expect((u&DENSE_USER_RAW)==DENSE_USER_RAW,0))bad_user(); // a raw user here: the slots were not resolved
      hand_event(myEv,pool,sl.k,(u&LOCAL_USER)?trans[sl.thread][u&~LOCAL_USER]:u,sl.cat);++others;if(stats)++rdWhy[why_table(u)];});
    rdHandEv+=others;rdSlotEv+=inRuns+others;
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
     sweep_events(lo,hi,false); // the next stretch may still be filling: runs end with this one
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
    if(rawSlots){
     #pragma omp for schedule(dynamic,1) nowait
     for(int t=0;t<T;++t){
      for(const ESmall& r:sideRows[t]){const uint32_t u=r.user;
       hand_event(myEv,pool,r.k,(u&LOCAL_USER)?trans[r.thread][u&~LOCAL_USER]:u,r.cat);if(stats)++rdWhy[why_table(u)];}
      rdHandEv+=sideRows[t].size();rdSlotEv+=sideRows[t].size();
     }
    }
    else{
     #pragma omp for schedule(dynamic,1) nowait
     for(size_t lo=0;lo<evEnd;lo+=ECH)sweep_events(lo,min(evEnd,lo+ECH),true);
    }
    #pragma omp for schedule(dynamic,1)
    for(size_t lo=0;lo<clEnd;lo+=CCH)sweep_clicks(lo,min(clEnd,lo+CCH));
   }
  }
  if(bucketMode){dense_note_end(denseEvEnd,(uint32_t)fillEndE);dense_note_end(denseClEnd,(uint32_t)fillEndC);} // one past the last stretch written
  __mark("DENSE1");
 }
 const size_t idxEnd=slots&&denseUsers?(size_t)denseMaxIdx.load()+1:0;
 optThrStats=stats;
 const double thrS0=stats?now_s():0; // start of the DENSE stage's serial lead-in
 double thrR0=0,thrA0=0,thrSingle=0; // parallel region start; AGGREGATE start; serial part at the end of DENSE
 vector<ThrStat> dstat(stats?T:0),astat(stats?T:0);
 // One line per stage, all times in ms. min/max/sum are over the threads.
 const auto thr_line=[&](const char* name,const vector<ThrStat>& v,double t0,double serialPre,double serialEnd){
  double fMin=1e9,fMax=0,lMin=1e9,lMax=0,wMin=1e9,wMax=0,wSum=0,cLoop=0,wr=0,wrMax=0,wrCpu=0,op=0,gr=0,tMin=1e9,tMax=0,cAll=0,en=0;unsigned wc=0,gc=0,idle=0;
  for(const ThrStat& x:v){
   if(x.first<0){++idle;cAll+=x.cOut-x.cIn;wr+=x.wWall;wrCpu+=x.wCpu;wc+=x.wCalls;op+=x.oWall;gr+=x.gWall;gc+=x.gCalls;en+=x.endWall;continue;}
   const double f=x.first-t0,l=x.last-t0,w=x.last-x.first,tl=x.out-x.last;
   fMin=min(fMin,f);fMax=max(fMax,f);lMin=min(lMin,l);lMax=max(lMax,l);wMin=min(wMin,w);wMax=max(wMax,w);wSum+=w;cLoop+=x.cLast-x.cFirst;
   wr+=x.wWall;wrMax=max(wrMax,x.wMax);wrCpu+=x.wCpu;wc+=x.wCalls;op+=x.oWall;gr+=x.gWall;gc+=x.gCalls;tMin=min(tMin,tl);tMax=max(tMax,tl);cAll+=x.cOut-x.cIn;en+=x.endWall;
  }
  if(idle==v.size())fMin=lMin=wMin=tMin=0;
  char b[640];snprintf(b,sizeof b,"STATS %s threads: n %zu noloop %u serial_pre %.2f first_start %.2f %.2f last_end %.2f %.2f loop_wall %.2f %.2f %.2f loop_cpu %.2f write_wall %.2f write_max %.2f write_cpu %.2f write_calls %u open_wall %.2f grow_wall %.2f grow_calls %u tail_wall %.2f %.2f end_wall %.2f serial_end %.2f thread_cpu %.2f\n",
   name,v.size(),idle,serialPre*1e3,fMin*1e3,fMax*1e3,lMin*1e3,lMax*1e3,wMin*1e3,wMax*1e3,wSum*1e3,cLoop*1e3,wr*1e3,wrMax*1e3,wrCpu*1e3,wc,op*1e3,gr*1e3,gc,tMin*1e3,tMax*1e3,en*1e3,serialEnd*1e3,cAll*1e3);
  cerr<<b;};
 uint64_t deferLoE=UINT64_MAX,deferHiE=0,deferLoC=UINT64_MAX,deferHiC=0; // counter ranges holding the rows of deferred indexes
 // ---- Per person bucket: order, sessionize, attribute. Each thread writes its own Parquet file. ----
 if(earlyOut)while(!earlyOut->done.load(memory_order_acquire))this_thread::yield(); // SS_OUTOPEN=early: long done in practice
 if(!(earlyOut&&earlyOut->dirs)){fs::create_directories(output/"user_daily");fs::create_directories(output/"campaign_daily");}
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
 // campaign_daily: the totals of all threads added up, one row per campaign and day with a session or an assist,
 // one file. By default one worker does this as soon as every thread has left the last sessionizing loop, while
 // the others flush and close their user_daily files and hand memory back (it used to follow the parallel
 // region on one thread: the WRITE stage). SS_CAMPWRITE=end: after the region, as before. Same bytes either way.
 static const bool campEarly=!(getenv("SS_CAMPWRITE")&&string_view(getenv("SS_CAMPWRITE"))=="end");
 string campCsv; // SS_DEBUG_CSV rows
 vector<Totals> campTotals(ncamp*(size_t)ndays);
 const auto merge_totals=[&](size_t lo,size_t hi){ // cells [lo,hi) of every thread's totals added into campTotals
  for(const Totals* tt:threadTotals)if(tt)for(size_t i=lo;i<hi;++i){campTotals[i].sessions+=tt[i].sessions;campTotals[i].purchasing_sessions+=tt[i].purchasing_sessions;campTotals[i].revenue+=tt[i].revenue;}
  for(const uint64_t* ta:threadAssists)if(ta)for(size_t i=lo;i<hi;++i)campTotals[i].assists+=ta[i];
 };
 static constexpr size_t MERGE_BLOCK=4096;
 const auto write_campaigns=[&]{
  if(!campEarly)merge_totals(0,ncells); // otherwise all threads have added the totals up, a block of cells each
  string& csv=campCsv;
  const auto open_camp=[&](PqFile& f){f.open(output/"campaign_daily"/"part-00000.parquet",{{"campaign_id",PQ_BYTES,false,CONV_UTF8},{"channel",PQ_BYTES,false,CONV_UTF8},{"day",PQ_INT32,false,CONV_DATE},
   {"sessions",PQ_INT64,false,CONV_NONE},{"purchasing_sessions",PQ_INT64,false,CONV_NONE},{"revenue_cents",PQ_INT64,false,CONV_NONE},{"assisted_sessions",PQ_INT64,false,CONV_NONE}});};
  if(campEarly){
   // The same column bodies as below, written through pointers into buffers with room for a row of every cell
   // (a length-prefixed text is its 32-bit little-endian length and its bytes; the numbers are little-endian as stored).
   size_t idBytes=0,chBytes=0;for(size_t ci=0;ci<ncamp;++ci){idBytes+=4+campaigns[ci].id.size();chBytes+=4+campaigns[ci].channel.size();}
   const auto cidBuf=make_unique_for_overwrite<char[]>(idBytes*(size_t)ndays+1),chanBuf=make_unique_for_overwrite<char[]>(chBytes*(size_t)ndays+1);
   const auto dayBuf=make_unique_for_overwrite<int32_t[]>(ncells+1);
   const auto sBuf=make_unique_for_overwrite<int64_t[]>(ncells+1),pBuf=make_unique_for_overwrite<int64_t[]>(ncells+1),rBuf=make_unique_for_overwrite<int64_t[]>(ncells+1),aBuf=make_unique_for_overwrite<int64_t[]>(ncells+1);
   char* pc=cidBuf.get();char* ph=chanBuf.get();size_t n=0;
   for(size_t ci=0;ci<ncamp;++ci){
    const string& id=campaigns[ci].id;const string& ch=campaigns[ci].channel;const uint32_t il=(uint32_t)id.size(),cl=(uint32_t)ch.size();
    for(int d=0;d<ndays;++d){const auto& t=campTotals[(size_t)d*ncamp+ci];if(t.sessions==0&&t.assists==0)continue;
     memcpy(pc,&il,4);memcpy(pc+4,id.data(),il);pc+=4+il;memcpy(ph,&cl,4);memcpy(ph+4,ch.data(),cl);ph+=4+cl;
     dayBuf[n]=(int32_t)(firstday+d);sBuf[n]=(int64_t)t.sessions;pBuf[n]=(int64_t)t.purchasing_sessions;rBuf[n]=(int64_t)t.revenue;aBuf[n]=(int64_t)t.assists;++n;
     if(debugCsv){csv+=hexstr(id);csv+=',';csv+=hexstr(ch);csv+=',';csv+=to_string(firstday+d);csv+=',';csv+=to_string(t.sessions);csv+=',';csv+=to_string(t.purchasing_sessions);csv+=',';csv+=to_string(t.revenue);csv+=',';csv+=to_string(t.assists);csv+='\n';}
    }
   }
   PqFile f;open_camp(f);
   if(n)f.row_group((int64_t)n,{string_view(cidBuf.get(),(size_t)(pc-cidBuf.get())),string_view(chanBuf.get(),(size_t)(ph-chanBuf.get())),string_view((const char*)dayBuf.get(),n*sizeof(int32_t)),
    string_view((const char*)sBuf.get(),n*sizeof(int64_t)),string_view((const char*)pBuf.get(),n*sizeof(int64_t)),string_view((const char*)rBuf.get(),n*sizeof(int64_t)),string_view((const char*)aBuf.get(),n*sizeof(int64_t))});
   f.close();
   return;
  }
  string cid,chan;vector<int32_t> days;vector<int64_t> s,p,r,a;
  for(size_t ci=0;ci<ncamp;++ci)for(int d=0;d<ndays;++d){const auto& t=campTotals[(size_t)d*ncamp+ci];if(t.sessions==0&&t.assists==0)continue;
   put_bytes(cid,campaigns[ci].id);put_bytes(chan,campaigns[ci].channel);days.push_back((int32_t)(firstday+d));
   s.push_back((int64_t)t.sessions);p.push_back((int64_t)t.purchasing_sessions);r.push_back((int64_t)t.revenue);a.push_back((int64_t)t.assists);
   if(debugCsv){csv+=hexstr(campaigns[ci].id);csv+=',';csv+=hexstr(campaigns[ci].channel);csv+=',';csv+=to_string(firstday+d);csv+=',';csv+=to_string(t.sessions);csv+=',';csv+=to_string(t.purchasing_sessions);csv+=',';csv+=to_string(t.revenue);csv+=',';csv+=to_string(t.assists);csv+='\n';}
  }
  PqFile f;open_camp(f);
  if(!days.empty())f.row_group((int64_t)days.size(),{cid,chan,raw(days),raw(s),raw(p),raw(r),raw(a)});
  f.close();
 };
 // Input files nobody reads any more are unmapped by the worker threads, a few large pieces each,
 // instead of leaving every page of them to process exit, which tears mappings down on one thread.
 vector<pair<char*,size_t>> unmapFirst,unmapLast;
 if(!getenv("SS_NOEARLYUNMAP")){ // measuring switch
  const bool eventsFree=!anyLongName.load()&&extraNames.empty(); // otherwise a person's name may still point into an event file
  group_pieces(eventFiles,(size_t)T,eventsFree?unmapFirst:unmapLast);
  group_pieces(clickFiles,(size_t)T,unmapLast); // click IDs still break ties while sessions are attributed
  group_pieces(linkFiles,1,unmapLast);
  // The slot arrays are not read after the DENSE stage; the part of them that was written is handed
  // back by the workers too (in pieces of whole huge pages) instead of at process exit.
  const auto slot_pieces=[&](const char* base,size_t bytes){
   const size_t huge=size_t(2)<<20,len=(bytes+huge-1)&~(huge-1),per=max(huge,(len/(size_t)T+huge-1)&~(huge-1));
   for(size_t at=0;at<len;at+=per)unmapLast.push_back({(char*)base+at,min(per,len-at)});};
  if(evBase&&evEnd)slot_pieces(evBase,(size_t)denseEvEnd.load()<<evShift);
  if(denseCl&&clEnd)slot_pieces((const char*)denseCl,(size_t)denseClEnd.load()*sizeof(CSlot));
 }
 // SS_UNMAP (measuring switch; default: the workers unmap, the event files before the index loop, the rest
 // after the last loop):
 //   bg       one helper thread unmaps both lists in 16 MB steps while the workers compute (the event files
 //            from the start of this stage, the rest from the end of the last loop; what is left at the end
 //            goes with the process). No worker waits in munmap, and a worker's mmap waits for one step at most.
 //   bgfirst  the helper takes the event files only; the rest as today.
 //   late     the workers unmap everything after the last loop, nothing before the index loop.
 vector<pair<char*,size_t>> unmapLastBg;
 {const char* v=getenv("SS_UNMAP");const string_view w(v?v:"");const int mode=(w=="bg"||w.empty())?1:w=="bgfirst"?2:w=="late"?3:0; // default bg (scoring machine: DENSE -3 ms, AGGREGATE -4 ms at 20M; -10 and -13 at 60M); SS_UNMAP=workers = the workers unmap
  const auto unmap_bg=[](vector<pair<char*,size_t>> v){if(v.empty())return;
   thread([v=std::move(v)]{const size_t step=size_t(16)<<20;for(const auto& r:v)for(size_t at=0;at<r.second;at+=step)munmap(r.first+at,min(step,r.second-at));}).detach();};
  // SS_POOLPRE=1 (measuring switch): every thread's pool gets its next region here, before any unmapping starts,
  // so the workers' first takes of this stage need no mmap while other threads hold the mapping lock in munmap.
  const bool poolPre=getenv("SS_POOLPRE")!=nullptr;
  if(poolPre)for(int t=0;t<T;++t)surv[t].pool.ensure(size_t(8)<<20);
  if(stats)cerr<<"STATS pools: SS_POOLPRE="<<poolPre<<"\n";
  if(mode==3){unmapLast.insert(unmapLast.end(),unmapFirst.begin(),unmapFirst.end());unmapFirst.clear();}
  if(mode==1||mode==2){unmap_bg(std::move(unmapFirst));unmapFirst.clear();}
  if(mode==1)unmapLastBg.swap(unmapLast);
  if(stats)cerr<<"STATS unmap: SS_UNMAP="<<(mode==1?"bg":mode==2?"bgfirst":mode==3?"late":"workers")<<" first "<<unmapFirst.size()<<" last "<<unmapLast.size()<<" helper-last "<<unmapLastBg.size()<<"\n";
 }
 if(stats)thrR0=now_s();
 // Turns of the index loop. On generated data the user index is a rank: the first five indexes hold about 1.1%
 // of all events each, the first 512 a sixth, the first 4096 over a quarter. With 512 indexes per turn the
 // first turn alone was more than a thread's share of the stage, so the low indexes get turns of their own
 // (the first 64 one by one, then 16 and 64 at a time). SS_TURNS=old: 512 everywhere.
 vector<uint32_t> turnLo;
 {static const bool turnsOld=[]{const char* v=getenv("SS_TURNS");return v&&!strcmp(v,"old");}();
  for(size_t i=0;i<idxEnd;){turnLo.push_back((uint32_t)i);const size_t step=turnsOld||i>=4096?ICH:i<64?1:i<512?16:64;i=min(idxEnd,i+step);}
  turnLo.push_back((uint32_t)idxEnd);}
 const size_t nTurns=turnLo.size()-1;
 #pragma omp parallel num_threads(T) reduction(+:num_sessions,rdHandEv,rdHandCl,rdFastIdx,rdFastEv,rdFastCl,rdDeferIdx,rdWhy[:9],rdMarkIdx,rdMarkPersons,rdMarkEv,rdMarkCl,rdMarkRows,rdMarkSorted,rdMarkPulled)
 {
  const int tid=omp_get_thread_num();
  ThrStat thrNone;ThrStat* cur=stats?&dstat[tid]:&thrNone; // the stage this thread is in
  double grow0=tlGrowWall;uint32_t growN0=tlGrowCalls;
  if(stats){cur->in=now_s();cur->cIn=thr_cpu_s();}
  const auto thr_chunk_in=[&]{if(__builtin_expect(stats,0)&&cur->first<0){cur->first=cur->last=now_s();cur->cFirst=cur->cLast=thr_cpu_s();}};
  const auto thr_chunk_out=[&]{if(__builtin_expect(stats,0)){cur->last=now_s();cur->cLast=thr_cpu_s();}};
  const auto thr_stage_out=[&]{cur->out=now_s();cur->cOut=thr_cpu_s();cur->gWall=tlGrowWall-grow0;cur->gCalls=tlGrowCalls-growN0;grow0=tlGrowWall;growN0=tlGrowCalls;};
  #pragma omp for schedule(static,1) nowait
  for(size_t i=0;i<unmapFirst.size();++i)munmap(unmapFirst[i].first,unmapFirst[i].second);
  // Per-thread campaign totals, laid out [day][campaign].
  // Everything this thread needs comes from its pool (fresh, so zeroed): totals, sort buffers, row buffers.
  HugePool& pool=surv[tid].pool;
  Totals* const tot=(Totals*)pool.take(ncells*sizeof(Totals));threadTotals[tid]=tot;
  uint64_t* const assists=(uint64_t*)pool.take(ncells*sizeof(uint64_t));threadAssists[tid]=assists;
  vector<uint32_t> freq(ncamp),positions(ncamp);vector<uint16_t> active(ncamp+1);size_t nact=0; // active: one free place after a full list (see the fast lane's large click sets)
  vector<uint64_t> catsum(catnames.size());vector<uint16_t> touched;
  PqFile file;
  auto open_file=[&]{char name[32];snprintf(name,sizeof name,"part-%05d.parquet",tid);
   const int efd=earlyOut&&(size_t)tid<earlyOut->fd.size()?earlyOut->fd[(size_t)tid]:-1; // each worker touches only its own entry
   if(efd>=0){earlyOut->fd[(size_t)tid]=-1;file.adopt(efd,userCols);}
   else file.open(output/"user_daily"/name,userCols);};
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
   const double w0=stats?now_s():0,c0=stats?thr_cpu_s():0;
   if(!file.is_open())open_file();
   const double w1=stats?now_s():0;
   file.row_group((int64_t)n,{string_view(uidp,uidLen),string_view((const char*)dayp,n*4),
    narrow?string_view((const char*)sess32p,n*4):string_view((const char*)sessp,n*8),
    narrow?string_view((const char*)evs32p,n*4):string_view((const char*)evsp,n*8),
    string_view((const char*)revp,n*8),catbody});
   if(stats){const double w2=now_s();cur->oWall+=w1-w0;cur->wWall+=w2-w1;cur->wMax=max(cur->wMax,w2-w1);cur->wCpu+=thr_cpu_s()-c0;++cur->wCalls;}
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
  // The rows of one person in one go (the fast lane; its names are always of the fixed kind).
  struct RowOut{uint64_t rev,best;int day;uint32_t sess,evs;uint16_t top;};
  // (SS_ROWS=old: one loop that does everything for a row, as before.)
  auto put_rows_old=[&nrows,&uidCap,&uidLen,&uidp,&grow_uid,&catv,&dbg,&catnames,dayp,revp,sess32p,evs32p,sessp,evsp,catbits,narrow,debugCsv,firstday,tid]
   (const NameRef& nm,const RowOut* R,size_t k){
   const size_t room=k*((size_t)nm.L+4)+64;
   if(uidCap-uidLen<room)grow_uid(room);
   size_t r=nrows;char* o=uidp+uidLen;
   for(size_t i=0;i<k;++i,++r){const RowOut& x=R[i];
    memcpy(o,&nm.L,4);memcpy(o+4,nm.p,16);o+=4+nm.L;
    dayp[r]=(int32_t)(firstday+x.day);revp[r]=(int64_t)x.rev;
    if(narrow){sess32p[r]=(int32_t)x.sess;evs32p[r]=(int32_t)x.evs;}else{sessp[r]=(int64_t)x.sess;evsp[r]=(int64_t)x.evs;}
    if(__builtin_expect(x.best!=0,0)){catbits[r>>3]|=(uint8_t)(1u<<(r&7));put_bytes(catv,catnames[x.top]);}
    if(debugCsv){string& t=dbg[tid];
     t+=hexstr(string_view(nm.p,nm.L));t+=',';t+=to_string(firstday+x.day);t+=',';t+=to_string(x.sess);t+=',';t+=to_string(x.evs);t+=',';t+=to_string(x.rev);t+=',';
     t+=x.best==0?string("NULL"):hexstr(catnames[x.top]);t+='\n';
    }
   }
   uidLen=(size_t)(o-uidp);nrows=r;
  };
  // The parts of a person's rows that few rows have: a top category (rows with a purchase), the test output.
  auto put_rows_rare=[&catv,&dbg,&catnames,catbits,debugCsv,firstday,tid](const NameRef& nm,const RowOut* R,size_t k,size_t r) __attribute__((noinline)) {
   for(size_t i=0;i<k;++i,++r){const RowOut& x=R[i];
    if(x.best!=0){catbits[r>>3]|=(uint8_t)(1u<<(r&7));put_bytes(catv,catnames[x.top]);}
    if(debugCsv){string& t=dbg[tid];
     t+=hexstr(string_view(nm.p,nm.L));t+=',';t+=to_string(firstday+x.day);t+=',';t+=to_string(x.sess);t+=',';t+=to_string(x.evs);t+=',';t+=to_string(x.rev);t+=',';
     t+=x.best==0?string("NULL"):hexstr(catnames[x.top]);t+='\n';
    }
   }
  };
  // The fixed-width columns and the name of every row in a loop that calls nothing and reads only locals
  // (the byte stores of the name could be stores to anything, so captured pointers would be read again for
  // every row); the rest in put_rows_rare, for persons that have any of it.
  auto put_rows=[&nrows,&uidCap,&uidLen,&uidp,&grow_uid,&put_rows_rare,dayp,revp,sess32p,evs32p,sessp,evsp,narrow,debugCsv,firstday]
   (const NameRef& nm,const RowOut* R,size_t k) __attribute__((always_inline)) {
   const uint32_t L=nm.L;const size_t room=k*((size_t)L+4)+64;
   if(__builtin_expect(uidCap-uidLen<room,0))grow_uid(room);
   const size_t r0=nrows;char* o=uidp+uidLen;
   const __m128i name=_mm_loadu_si128((const __m128i*)(const void*)nm.p);
   const int32_t day0=(int32_t)firstday;uint64_t anyBest=0;
   if(narrow){int32_t* const dp=dayp+r0;int64_t* const rp=revp+r0;int32_t* const sp=sess32p+r0;int32_t* const ep=evs32p+r0;
    for(size_t i=0;i<k;++i){const RowOut& x=R[i];
     memcpy(o,&L,4);_mm_storeu_si128((__m128i*)(void*)(o+4),name);o+=4+L;
     dp[i]=day0+(int32_t)x.day;rp[i]=(int64_t)x.rev;sp[i]=(int32_t)x.sess;ep[i]=(int32_t)x.evs;anyBest|=x.best;}}
   else{int32_t* const dp=dayp+r0;int64_t* const rp=revp+r0;int64_t* const sp=sessp+r0;int64_t* const ep=evsp+r0;
    for(size_t i=0;i<k;++i){const RowOut& x=R[i];
     memcpy(o,&L,4);_mm_storeu_si128((__m128i*)(void*)(o+4),name);o+=4+L;
     dp[i]=day0+(int32_t)x.day;rp[i]=(int64_t)x.rev;sp[i]=(int64_t)x.sess;ep[i]=(int64_t)x.evs;anyBest|=x.best;}}
   uidLen=(size_t)(o-uidp);nrows=r0+k;
   if(__builtin_expect(anyBest!=0||debugCsv,0))put_rows_rare(nm,R,k,r0);
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
   // The user word of an event slot holding a valid event (with rawSlots the slots are as the parser left them).
   RawCtx rawCtx;rawCtx.v3=hasV3;
   const auto ucode=[&rawCtx,rawSlots](const ESmall& sl)->uint32_t{return rawSlots?rawCtx.code(sl):sl.user;};
   // A click of the fast lane: time and campaign as in Click, and its counter, which stands for its ID.
   // (A click slot itself; in a gathered copy `present` holds the counter. When an index's clicks fill their
   // stretch of counters without a gap - no empty slot, nobody else's click: as many clicks as slots - the
   // slots are used where they lie, nothing is copied, and the counter of a click is its place in the stretch.)
   using FClick=CSlot;
   static const bool noDirectClicks=getenv("SS_TEST_NODIRECT")!=nullptr; // test switch: always gather
   const bool wideWindow=(uint64_t)(window_end-clickStart)>>32!=0; // click times beyond 32 bits: the clicks themselves are sorted, in a copy
   Scratch<Ev> fastE;Scratch<FClick> fastC;fastE.pool=&pool;fastC.pool=&pool;
   // One index's sessions (start time; number of its rows before the session) and purchases
   // (session<<40 | category<<24 | price), written by the walk over its event slots.
   Scratch<uint64_t> sessTs,purBuf,clickKeys,clickKeys2,clickKeys3;Scratch<uint32_t> sessOrd;Scratch<FClick> fastC2;sessTs.pool=&pool;purBuf.pool=&pool;sessOrd.pool=&pool;clickKeys.pool=&pool;clickKeys2.pool=&pool;clickKeys3.pool=&pool;fastC2.pool=&pool;
   uint32_t* const sortCnt=(uint32_t*)pool.take((CLICK_SORT_BUCKETS+1)*sizeof(uint32_t)); // click_key_sort's counters
   vector<uint8_t> lastOfV(ncamp+1,0xFF);uint8_t* const lastOf=lastOfV.data(); // small click sets: last position of a campaign
   // Small click sets, set-up in four loops over the clicks instead of seven (SS_SMALLSET=old: the old set-up):
   // times and campaigns out of the clicks; ranks; every click straight to its place (clicks of one moment
   // show in the sum of the ranks and take the old way through keys and ID texts); for every place the
   // place of the campaign's click before it (lastG: "set's number << 8 | place" per campaign, so that
   // nothing has to be cleared after a set). A session then finds the first click of every campaign in
   // its window by one compare of those places with the window's first place.
   static const bool rowsOld=[]{const char* v=getenv("SS_ROWS");return v&&!strcmp(v,"old");}(); // the fast lane's row writer as it was
   static const bool smallOld=[]{const char* v=getenv("SS_SMALLSET");return v&&!strcmp(v,"old");}();
   vector<uint32_t> lastGV(ncamp+1,0);uint32_t* const lastG=lastGV.data();uint32_t smallGen=0;
   alignas(16) static constexpr auto PLACE16=[]{array<array<int8_t,16>,17> a{};for(int v=0;v<17;++v)for(int i=0;i<16;++i)a[(size_t)v][(size_t)i]=(int8_t)v;return a;}();
   // The 32-bit click arithmetic below counts times from the window start (clicks: -7 days and up), so
   // the window has to be shorter than 2^31 ms; otherwise all click sets take the general way.
   const bool smallClicksOk=(uint64_t)(window_end-window_start)<0x7fffffffull&&!getenv("SS_TEST_NOSMALLCLICKS");
   uint32_t* const fr=freq.data();uint32_t* const pos=positions.data();uint16_t* const act=active.data();
   // Large click sets: assists are counted per row, not per session. since[c] is the number of the first
   // session of the current row that campaign c was in the window for (set when its first click comes
   // into the window and when a row begins); when its last click leaves the window, and when the row is
   // closed, the sessions since then are added in one step. No list of active campaigns is kept: a row
   // is closed by a walk over the clicks in the window, which leaves since[c] at the closing session, so
   // a second click of a campaign adds nothing. The window runs over 8-byte keys "time << 28 | position
   // << 16 | campaign", which are also what is sorted (up to 4096 clicks; the position is only looked at
   // for clicks of one moment, to order them by their ID texts): the clicks are not copied in order.
   // SS_LARGEASSIST=old: keys with positions, clicks copied out in order, the
   // active list, one ++ per session and active campaign.
   static const bool largeOld=[]{const char* v=getenv("SS_LARGEASSIST");return v&&!strcmp(v,"old");}();
   static const size_t largeMax=[]{const char* v=getenv("SS_TEST_LARGEMAX");const long x=v?atol(v):4096;return (size_t)(x<1?1:x>4096?4096:x);}(); // tests: sets beyond this take the path of sets beyond 4096 clicks
   static const bool assistLoop=[]{const char* v=getenv("SS_SMALLASSIST");return v&&!strcmp(v,"loop");}(); // small sets: the row's counters added by a loop over the clicks (old) instead of eight at a time
   vector<uint32_t> sinceV(ncamp+1,0);uint32_t* const since=sinceV.data();
   static const bool clickSortOld=[]{const char* v=getenv("SS_CLICKSORT");return v&&!strcmp(v,"old");}();
   vector<uint32_t> bigHist(2048);
   vector<RowOut> rowsOut((size_t)ndays+2);
   const atomic<uint32_t>* const comp=denseComp;const uint32_t dBase=denseBase;
   // ---- Indexes finished here although they are not fast: marked ones, and ones with several persons. ----
   // A user index with a row in the general lane (an event or click whose own ID is not a dense one: about
   // five rows per million, so a user with 200,000 rows has one more often than not) used to be handed to the
   // general lane with all its rows, and so was an index whose aliases are not all one person. The biggest
   // users are exactly those, they sit next to each other at the lowest indexes, and so they all fell into
   // one person bucket of the last stage: one thread. Now such an index is finished here, by the thread that
   // has it, if it has events in the slots. For each of its persons that has events in the slots:
   //   - the person's events are copied out of the slots and its general-lane events (genRefs) are added;
   //     those are turned into tombstones in the general lists, so the last stage does not see the person at
   //     all (its general-lane clicks stay in the lists and are dropped there, as clicks of a person without
   //     events);
   //   - the events are put in time order: they almost are, so the rows that come too early are taken out,
   //     sorted and merged back in (counting passes over everything if that is more than an eighth of them);
   //   - its clicks, slots and general ones, are ordered through keys "time << 24 | position" by counting
   //     passes; clicks of one moment are then ordered by their ID texts, larger first (click_before);
   //   - sessionize_person, the general lane's per-person function, does the rest.
   // A person of the index without any event in the slots is not finished here: if it has general-lane
   // events, the general lane keeps them and is handed the person's slot clicks; if it has none, it has no
   // event at all and nothing is written for it.
   // A click time or count beyond the key returns false before anything is changed, and the index is handed
   // over as before. SS_MARKED=old: marked indexes are always handed over. SS_SEVERAL=old: indexes with
   // several persons are.
   static const bool markedOld=[]{const char* v=getenv("SS_MARKED");return v&&!strcmp(v,"old");}();
   const bool markedOn=!markedOld&&genRefsComplete; // (without the notes a marked index cannot be finished here)
   struct KeyOnly{uint64_t key;};
   Scratch<Ev> mkE,mkX;Scratch<uint64_t> mkK,mkK2,mkSrc;Scratch<uint32_t> mkCtr;Scratch<KeyOnly> mkOut;
   mkE.pool=mkX.pool=&pool;mkK.pool=mkK2.pool=mkSrc.pool=&pool;mkCtr.pool=&pool;mkOut.pool=&pool;
   vector<uint32_t> mkHist(2048),mkSideEvP;vector<const Click*> mkSide,mkSel;vector<Ev*> mkSideEv;
   static const bool severalOld=[]{const char* v=getenv("SS_SEVERAL");return v&&!strcmp(v,"old");}();
   const auto marked_index=[&mkE,&mkX,&mkK,&mkK2,&mkSrc,&mkCtr,&mkOut,&mkHist,&mkSide,&mkSideEv,&mkSideEvP,&mkSel,&sessionize_person,&person_name,&rdMarkIdx,&rdMarkPersons,&rdMarkEv,&rdMarkCl,&rdMarkRows,&rdMarkSorted,&rdMarkPulled,&rdHandCl,
     &pool,&txt,myCl,hand_click,ucode,ev_at,bucket_end,dBase,cls=(const CSlot*)denseCl,kc=dense.kc,gref=genRefs.data(),ngref=genRefs.size()]
    (size_t ix,uint32_t ec,uint32_t cc,size_t ef,size_t el,size_t cf,size_t cl,uint32_t stored) __attribute__((noinline)) ->bool{
    const uint32_t code=DENSE_USER|(uint32_t)(ix<<dn::ALIAS_BITS);
    const auto root_of=[stored](uint32_t alias)->uint32_t{return alias^((stored>>(3*alias))&7);}; // smallest alias of the alias's group
    const auto roots_of=[&root_of](uint32_t aliases)->uint32_t{uint32_t r=0;for(uint32_t m=aliases&255u;m;m&=m-1)r|=1u<<root_of((uint32_t)__builtin_ctz(m));return r;};
    const uint32_t person0=dBase+(uint32_t)(ix<<dn::ALIAS_BITS); // + root: the index's persons
    // The index's general-lane rows that are still alive, and whose they are.
    const GRef* g0=lower_bound(gref,gref+ngref,(uint32_t)ix,[](const GRef& a,uint32_t x){return a.ix<x;});
    const GRef* const gEnd=gref+ngref;
    mkSide.clear();mkSideEv.clear();mkSideEvP.clear();
    uint32_t sideEvRoots=0;uint64_t allKeys=0;
    for(const GRef* g=g0;g<gEnd&&g->ix==ix;++g){
     const uint32_t now=g->click?((const Click*)g->at)->person:((const Ev*)g->at)->person;
     if(now!=g->person)continue; // lost to an earlier copy in the dedup: a tombstone
     if(g->click){mkSide.push_back((const Click*)g->at);allKeys|=((const Click*)g->at)->key;}
     else{mkSideEv.push_back((Ev*)g->at);mkSideEvP.push_back(g->person);sideEvRoots|=1u<<((g->person-person0)&7u);}
    }
    const size_t nxe=mkSideEv.size(),nxc=mkSide.size();
    // Events out of the slots: those of the aliases in `want`. (A slot is laid out like an Ev: the key with its
    // bias, the user where the person is, the category. sessionize_person reads the key and the category only,
    // so the copy is one load, one subtraction and one store.) seen: the aliases of all the index's events.
    static_assert(sizeof(ESmall)==sizeof(Ev)&&offsetof(ESmall,k)==offsetof(Ev,k)&&offsetof(ESmall,cat)==offsetof(Ev,cat));
    Ev* const E=mkE.get((size_t)ec+nxe+1);
    const auto gather_events=[&](uint32_t want,uint32_t& seen)->size_t{
     const __m128i bias=_mm_set_epi64x(0,(long long)PR_KBIAS);size_t n=0;uint32_t sn=0;
     for(size_t c=ef;c<el;++c){
      const ESmall& sl=ev_at(c);
      if(sl.k<PR_KBIAS)continue;
      const uint32_t u=ucode(sl);
      if((u^code)>=8)continue;
      const uint32_t bit=1u<<(u&7);sn|=bit;
      if(!(bit&want))continue;
      _mm_storeu_si128((__m128i*)(void*)(E+n),_mm_sub_epi64(_mm_loadu_si128((const __m128i*)(const void*)&sl),bias));++n;
     }
     seen=sn;return n;};
    // Clicks out of the slots, the same way: key and counter.
    uint64_t* const src=mkSrc.get((size_t)cc+nxc+1);uint32_t* const ctr=mkCtr.get((size_t)cc+1);
    const auto gather_clicks=[&](uint32_t want,uint32_t& seen)->size_t{
     size_t m=0;uint32_t sn=0;uint64_t keys=0;
     for(size_t c=cf;c<cl;++c){
      const CSlot& sl=cls[c];const uint32_t u=sl.user;
      if(!sl.present||(u^code)>=8)continue;
      const uint32_t bit=1u<<(u&7);sn|=bit;
      if(!(bit&want))continue;
      src[m]=sl.key;ctr[m]=(uint32_t)c;keys|=sl.key;++m;
     }
     allKeys|=keys;seen=sn;return m;};
    uint32_t evAliases=0,clAliases=0;
    size_t n=gather_events(255u,evAliases),m=gather_clicks(255u,clAliases);
    if(__builtin_expect(n!=ec||m!=cc,0)){fprintf(stderr,"ERROR dense slots changed under the reader\n");_exit(1);}
    if((size_t)cc+nxc>=(size_t(1)<<24)||(allKeys>>16)>>40)return false; // the click key below has 24 bits for the position and 40 for the time
    const uint32_t evRoots=roots_of(evAliases);
    // One person has all the slot rows: the common case. (General-lane rows of another person of the index, which
    // then has no row in the slots at all, simply stay with the general lane.)
    const bool single=(evRoots&(evRoots-1))==0&&(roots_of(clAliases)&~evRoots)==0;
    if(!single&&severalOld)return false;
    // From here on the index is finished here.
    // One person with events in the slots: E[0..n) and src/ctr[0..m) hold its slot rows.
    const auto finish_person=[&](uint32_t root,size_t n,size_t m) __attribute__((always_inline)) {
     const uint32_t person=person0+root;
     // Its general events come along and leave the general lists; its general clicks are only read.
     // (Whose an event is comes from the note, not from the list: a tombstone's number may be another person of
     // this index when the person buckets are narrower than an index, as tests make them.)
     size_t taken=0;
     for(size_t i=0;i<nxe;++i){if(mkSideEvP[i]!=person)continue;Ev* const g=mkSideEv[i];E[n]=*g;++n;g->person=bucket_end(person);++taken;}
     mkSel.clear();
     for(size_t i=0;i<nxc;++i)if(mkSide[i]->person==person){src[m+mkSel.size()]=mkSide[i]->key;mkSel.push_back(mkSide[i]);}
     const Click* const* const sel=mkSel.data();const size_t mAll=m+mkSel.size();
     // Time order: rows earlier than a row before them are taken out, sorted, and merged back from the end.
     // (Only the times are compared: rows of one moment may stay in any order.)
     {const size_t xcap=n/8+1;Ev* const X=mkX.get(xcap+1);size_t nk=0,nx=0,i=0;uint64_t mx=0;
      for(;i<n;++i){const uint64_t t=E[i].k>>EV_TS;if(t<mx)break;mx=t;} // (in order up to here: nothing to move)
      for(nk=i;i<n;++i){const uint64_t t=E[i].k>>EV_TS;
       if(t>=mx){mx=t;E[nk++]=E[i];}
       else{if(nx==xcap)break;X[nx++]=E[i];}}
      if(i<n){ // too many: everything back into one array and a full sort, by counting passes over the times
       memcpy((void*)(E+nk),(const void*)X,nx*sizeof(Ev));rdMarkSorted+=n;
       if(n<2048)sort(E,E+n,[](const Ev& a,const Ev& b){return a.k<b.k;});
       else{
        uint64_t tmin=~0ull,tmax=0;
        for(size_t q=0;q<n;++q){const uint64_t t=E[q].k>>EV_TS;tmin=min(tmin,t);tmax=max(tmax,t);}
        Ev* A=E;Ev* B=mkX.get(n+1);uint32_t* const h=mkHist.data();
        for(unsigned sh=0;sh<64&&((tmax-tmin)>>sh)!=0;sh+=11){
         memset(h,0,2048*sizeof(uint32_t));
         for(size_t q=0;q<n;++q)++h[(((A[q].k>>EV_TS)-tmin)>>sh)&2047];
         uint32_t at=0;for(size_t d=0;d<2048;++d){const uint32_t c=h[d];h[d]=at;at+=c;}
         for(size_t q=0;q<n;++q){const __m128i e=_mm_loadu_si128((const __m128i*)(const void*)(A+q));
          _mm_storeu_si128((__m128i*)(void*)(B+h[((((uint64_t)_mm_cvtsi128_si64(e)>>EV_TS)-tmin)>>sh)&2047]++),e);}
         swap(A,B);
        }
        if(A!=E)memcpy((void*)E,(const void*)A,n*sizeof(Ev));
       }
      }
      else if(nx){
       sort(X,X+nx,[](const Ev& a,const Ev& b){return a.k<b.k;});
       size_t a=nk,b=nx,o=n;
       while(b){if(a&&(E[a-1].k>>EV_TS)>(X[b-1].k>>EV_TS))E[--o]=E[--a];else E[--o]=X[--b];}
       rdMarkPulled+=nx;
      }}
     // Clicks in click_before order.
     KeyOnly* const out=mkOut.get(mAll+1);
     if(mAll){
      uint64_t* K=mkK.get(mAll+1);
      for(size_t i=0;i<mAll;++i)K[i]=((src[i]>>16)<<24)|i;
      K=time_pos_sort(K,mkK2.get(mAll+1),mAll,mkHist.data());
      // Clicks of one moment: the larger ID text first.
      const auto text_of=[&](uint64_t key,char* buf)->string_view{const size_t i=(size_t)(key&0xFFFFFFu);
       if(i<m)return string_view(buf,text36_fast('k',dn::value_of(ctr[i],kc),buf));
       const Click* const c=sel[i-m];return string_view(c->id,c->idlen);};
      for(size_t r0=0;r0<mAll;){
       const uint64_t t=K[r0]>>24;size_t r1=r0+1;
       while(r1<mAll&&(K[r1]>>24)==t)++r1;
       if(__builtin_expect(r1-r0>1,0)){ // (common for big users: click times are whole seconds and come in bursts)
        bool slotsOnly=true;for(size_t q=r0;q<r1;++q)slotsOnly&=(size_t)(K[q]&0xFFFFFFu)<m;
        if(slotsOnly){ // dense IDs: order36 of the ID's number orders as the texts do
         const auto ord=[&](uint64_t key){return order36(dn::value_of(ctr[key&0xFFFFFFu],kc));};
         if(r1-r0==2){if(ord(K[r0])<ord(K[r0+1]))swap(K[r0],K[r0+1]);}
         else sort(K+r0,K+r1,[&](uint64_t x,uint64_t y){return ord(x)>ord(y);});
        }
        else sort(K+r0,K+r1,[&](uint64_t x,uint64_t y){char bx[16],by[16];return text_of(x,bx)>text_of(y,by);});
       }
       r0=r1;
      }
      for(size_t i=0;i<mAll;++i)out[i].key=src[K[i]&0xFFFFFFu];
     }
     char tmp[16];
     sessionize_person(person_name(person,tmp),E,n,(const KeyOnly*)out,(const KeyOnly*)out+mAll);
     ++rdMarkPersons;rdMarkEv+=n;rdMarkCl+=mAll;rdMarkRows+=taken+mkSel.size();
    };
    if(single)finish_person((uint32_t)__builtin_ctz(evRoots),n,m);
    else{
     // Several persons: each one that has events in the slots is picked out of the slots again and finished.
     // A person with general events only stays with the general lane, which needs its slot clicks then.
     const auto aliases_of=[&root_of](uint32_t root)->uint32_t{uint32_t a=0;for(uint32_t x=0;x<8;++x)if(root_of(x)==root)a|=1u<<x;return a;};
     uint32_t seen;
     for(uint32_t rs=evRoots;rs;rs&=rs-1){
      const uint32_t root=(uint32_t)__builtin_ctz(rs),want=aliases_of(root);
      const size_t nr=gather_events(want,seen),mr=gather_clicks(want,seen);
      finish_person(root,nr,mr);
     }
     for(uint32_t rs=sideEvRoots&~evRoots;rs;rs&=rs-1){
      const uint32_t root=(uint32_t)__builtin_ctz(rs),want=aliases_of(root);
      for(size_t c=cf;c<cl;++c){const CSlot& sl=cls[c];const uint32_t u=sl.user;
       if(sl.present&&(u^code)<8&&((want>>(u&7))&1u)){hand_click(myCl,pool,txt,sl.key,person0+root,(uint32_t)c);++rdHandCl;}}
     }
    }
    ++rdMarkIdx;
    return true;
   };
   #pragma omp for schedule(dynamic,1) reduction(min:deferLoE,deferLoC) reduction(max:deferHiE,deferHiC)
   for(size_t tn=0;tn<nTurns;++tn){
    const size_t i0=turnLo[tn],i1=turnLo[tn+1];
    thr_chunk_in();
    for(size_t ix=i0;ix<i1;++ix){
     DInfo& f=info[ix];
     const uint32_t ec=f.ec.load(memory_order_relaxed),cc=f.cc.load(memory_order_relaxed);
     if(!(ec|cc))continue;
     const bool general=f.general.load(memory_order_relaxed)!=0||(testMarkN&&ix%testMarkN==0);
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
     else if(__builtin_expect(general,0)&&ec&&markedOn&&marked_index(ix,ec,cc,ef,el,cf,cl,stored))continue; // a marked index: finished here
     size_t n=0,m=0;Ev* E=nullptr;FClick* C=nullptr;uint32_t root=0;bool direct=false;
     const auto ctr_of=[&C,&direct,cf](uint32_t i)->uint32_t{return direct?(uint32_t)cf+i:C[i].present;}; // the counter of click i of C (before C is put in order)
     const uint64_t* SL=nullptr;uint64_t* mySL=nullptr;const uint64_t* pur=nullptr;size_t ns=0;uint64_t disorder=0; // SL: session entries "start << 24 | rows before", then the row count
     // The sweep's session list, if the index is one run of rows in time order.
     const uint64_t* const sweepList=f.multi.load(memory_order_relaxed)?nullptr:f.sess.load(memory_order_relaxed);
     const uint32_t sweepNs=f.ns.load(memory_order_relaxed);
     const bool listed=ec&&sweepList&&!(sweepNs>>31);
     if(fast&&ec>=(1u<<24)){fast=false;why=5;} // too many rows for a session entry: the general lane takes the index
     if(fast){
      uint32_t mask=0;
      if(listed){ns=sweepNs;n=ec;mask=f.amask.load(memory_order_relaxed);SL=sweepList;pur=sweepList+ns+1;}
      else{mySL=sessTs.get((size_t)ec+2);SL=mySL;}
      // Otherwise (several runs, or a row out of order) walk the index's stretch of event slots here
      // the way the sweep does, skipping other people's slots; `disorder` says whether a row was out of order.
      uint64_t* const myPur=listed?nullptr:purBuf.get((size_t)ec+2);size_t np=0;
      uint64_t last=0-(uint64_t)(GAP+1); // the first row starts a session
      if(!listed)for(size_t c=ef;c<el;++c){
       const ESmall& sl=ev_at(c);const uint64_t k=sl.k;
       if(k<PR_KBIAS)continue;
       const uint32_t u=ucode(sl);
       if((u^code)>=8)continue;
       mask|=1u<<(u&7);
       const uint64_t ts=(k-PR_KBIAS)>>EV_TS,step=ts-last;
       disorder|=step>>63;
       mySL[ns]=(ts<<24)|n;ns+=step>(uint64_t)GAP;last=ts;
       if(__builtin_expect((k&EV_PURCHASE)!=0,0))myPur[np++]=((uint64_t)(ns-1)<<40)|((uint64_t)sl.cat<<24)|(k&EV_PRICE);
       ++n;
      }
      if(!listed){mySL[ns]=n;myPur[np]=~0ull;pur=myPur;}
      direct=cc&&cl-cf==(size_t)cc&&cc<=4096u&&!wideWindow&&!noDirectClicks;
      if(direct){ // every slot of [cf, cl) is a click of this index (the sweep counted cc of them in there)
       C=denseCl+cf;m=cc;
       static constexpr uint8_t BIT8[8]={1,2,4,8,16,32,64,128}; // (a table: a shift by a variable count costs more here)
       uint32_t m0=0,m1=0;size_t i=0;
       for(;i+2<=m;i+=2){m0|=BIT8[C[i].user&7];m1|=BIT8[C[i+1].user&7];}
       if(i<m)m0|=BIT8[C[i].user&7];
       mask|=m0|m1;
      }
      else{
       C=fastC.get((size_t)cc+1);
       for(size_t c=cf;c<cl;++c){
        const CSlot& sl=denseCl[c];const uint32_t u=sl.user;
        const uint32_t ok=(uint32_t)(sl.present!=0)&(uint32_t)((u^code)<8);
        _mm_storeu_si128((__m128i*)(void*)(C+m),_mm_loadu_si128((const __m128i*)(const void*)&sl));C[m].present=(uint32_t)c; // key and user as they are
        mask|=ok<<(u&7);m+=ok;
       }
      }
      if(__builtin_expect(n!=ec||m!=cc,0)){fprintf(stderr,"ERROR dense slots changed under the reader\n");_exit(1);}
      // One person: every alias in use has the same smallest alias of its group.
      const uint32_t a0=(uint32_t)__builtin_ctz(mask|0x100u);root=a0^((stored>>(3*a0))&7);
      // (The group fields of all eight aliases against the root at once; SPREAD3 puts the aliases in use on the fields.)
      {const uint32_t x=(stored^COMP_IDENT)^(root*0x249249u),differs=(x|(x>>1)|(x>>2))&0x249249u;
       if(differs&SPREAD3[mask&255u]){fast=false;why=6;}}
     }
     if(fast){
      const uint32_t person=dBase+(uint32_t)((ix<<dn::ALIAS_BITS)|root);
      if(__builtin_expect(disorder!=0,0)){
       // Not in time order: copy the rows out, sort them, and make the session list from the copy.
       E=fastE.get((size_t)ec+1);size_t q=0;
       for(size_t c=ef;c<el;++c){const ESmall& sl=ev_at(c);
        if(sl.k>=PR_KBIAS&&(ucode(sl)^code)<8){E[q].k=sl.k-PR_KBIAS;E[q].person=person;E[q].cat=sl.cat;E[q].pad=0;++q;}}
       ev_time_sort(E,E+n);
       uint64_t* const sortedPur=purBuf.get((size_t)ec+2);size_t np=0;
       if(!mySL){mySL=sessTs.get((size_t)ec+2);}
       SL=mySL;
       ns=0;uint64_t last=0-(uint64_t)(GAP+1);
       for(size_t i=0;i<n;++i){
        const uint64_t k=E[i].k,ts=k>>EV_TS,step=ts-last;
        mySL[ns]=(ts<<24)|i;ns+=step>(uint64_t)GAP;last=ts;
        if(k&EV_PURCHASE)sortedPur[np++]=((uint64_t)(ns-1)<<40)|((uint64_t)E[i].cat<<24)|(k&EV_PRICE);
       }
       mySL[ns]=n;sortedPur[np]=~0ull;pur=sortedPur;
      }
      // The person's clicks. A small set (most are) is put in order once and kept as plain arrays: a
      // session then finds the clicks of its seven days with one vector compare, the credited campaign
      // is that of the last one, and the assists are counted in one byte per click (bumped only for the
      // first click of its campaign inside the window) and added to the totals when the row is done.
      // A larger set takes the sliding window over the sorted clicks, as the general lane does.
      enum{NONE,SMALL,LARGE,LARGEOLD} mode=m==0?NONE:(m<=16&&smallClicksOk)?SMALL:(largeOld||wideWindow)?LARGEOLD:LARGE; // (LARGE keeps times in 36 bits)
      alignas(16) int32_t cts[16];uint16_t ccamp[16],credit[17];uint16_t dupMask[17];size_t nvec=0;uint64_t cntLo=0,cntHi=0;
      alignas(16) int8_t prevPlace[16]; // of the campaign's click before this one, or -1
      if(mode==SMALL&&!smallOld){
       alignas(16) int32_t tm[16];alignas(16) uint8_t rnk[16];uint16_t cmp[16];
       const __m128i far=_mm_set1_epi32(INT32_MAX); // later than every click: times from the window start stay below it
       _mm_store_si128((__m128i*)tm,far);_mm_store_si128((__m128i*)(tm+4),far);_mm_store_si128((__m128i*)(tm+8),far);_mm_store_si128((__m128i*)(tm+12),far);
       {const FClick* const q=C;const size_t mm=m;
        for(size_t i=0;i<mm;++i){const uint64_t k=q[i].key;tm[i]=(int32_t)((int64_t)(k>>16)-WEEK);cmp[i]=(uint16_t)k;}}
       const __m128i T0=_mm_load_si128((const __m128i*)tm),T1=_mm_load_si128((const __m128i*)(tm+4)),T2=_mm_load_si128((const __m128i*)(tm+8)),T3=_mm_load_si128((const __m128i*)(tm+12));
       __m128i acc=_mm_setzero_si128();
       for(size_t j=0;j<m;++j){const __m128i b=_mm_set1_epi32(tm[j]); // one byte per place: all ones where that click is later than click j
        acc=_mm_sub_epi8(acc,_mm_packs_epi16(_mm_packs_epi32(_mm_cmpgt_epi32(T0,b),_mm_cmpgt_epi32(T1,b)),_mm_packs_epi32(_mm_cmpgt_epi32(T2,b),_mm_cmpgt_epi32(T3,b))));}
       _mm_store_si128((__m128i*)rnk,acc);
       // The rank of a click is the number of clicks before it in time. All moments different: the ranks are
       // 0..m-1. Two clicks of one moment share a rank and a later one is missing, so the sum is smaller
       // (the places without a click have rank m each).
       const __m128i sad=_mm_sad_epu8(acc,_mm_setzero_si128());
       const uint32_t rankSum=(uint32_t)_mm_cvtsi128_si32(sad)+(uint32_t)_mm_extract_epi16(sad,4);
       _mm_store_si128((__m128i*)cts,far);_mm_store_si128((__m128i*)(cts+4),far);_mm_store_si128((__m128i*)(cts+8),far);_mm_store_si128((__m128i*)(cts+12),far);
       memset(ccamp,0,sizeof ccamp); // (count_assists reads all sixteen places)
       if(__builtin_expect(rankSum==(uint32_t)(m*(m-1)/2+(16-m)*m),1)){
        const size_t mm=m;
        for(size_t i=0;i<mm;++i){const unsigned r=rnk[i];const uint16_t c=cmp[i];cts[r]=tm[i];ccamp[r]=c;credit[r+1]=c;}
       }
       else{
        // Clicks of one moment: by position first, then the larger ID text first, so that the last one has the smallest.
        uint64_t keys[16];uint8_t taken[16];memset(taken,0,16);
        for(size_t i=0;i<m;++i){const unsigned r=rnk[i];keys[r+taken[r]++]=((C[i].key>>16)<<32)|i;}
        for(size_t i=1;i<m;++i)if((keys[i]>>32)==(keys[i-1]>>32)){
         const auto text_order=[&](uint64_t key){return order36(dn::value_of(ctr_of((uint32_t)key),dense.kc));};
         const uint64_t key=keys[i];size_t j=i;
         while(j>0&&(keys[j-1]>>32)==(key>>32)&&text_order(keys[j-1])<text_order(key)){keys[j]=keys[j-1];--j;}
         keys[j]=key;
        }
        for(size_t i=0;i<m;++i){const uint16_t c=(uint16_t)C[(uint32_t)keys[i]].key;cts[i]=(int32_t)((int64_t)(keys[i]>>32)-WEEK);ccamp[i]=c;credit[i+1]=c;}
       }
       credit[0]=organic; // read when no click is in the window
       if(__builtin_expect(++smallGen==(1u<<24),0)){memset(lastG,0,(ncamp+1)*sizeof(uint32_t));smallGen=1;} // (set numbers used up: start again)
       {const uint32_t tag=smallGen<<8;const size_t mm=m;
        #pragma GCC unroll 1
        for(size_t i=0;i<mm;++i){const uint16_t c=ccamp[i];const uint32_t x=lastG[c]^tag,none=0u-(uint32_t)(x>255u); // x: the place, if the entry is of this set
         lastG[c]=tag|(uint32_t)i;prevPlace[i]=(int8_t)(x|none);}}
       nvec=(m+3)/4;
      }
      else if(mode==SMALL){
       uint64_t keys[16];
       // In time order, clicks of one moment by position, without a data-dependent branch: the place of a
       // click is the number of clicks with an earlier time (counted for all sixteen places at once, one
       // click after the other) plus the number of clicks of the same moment placed before it.
       {alignas(16) int32_t tm[16];alignas(16) uint8_t rnk[16];uint8_t taken[16];
        for(size_t i=0;i<16;++i)tm[i]=INT32_MAX; // later than every click: times from the window start stay below it
        for(size_t i=0;i<m;++i)tm[i]=(int32_t)((int64_t)(C[i].key>>16)-WEEK);
        const __m128i T0=_mm_load_si128((const __m128i*)tm),T1=_mm_load_si128((const __m128i*)(tm+4)),T2=_mm_load_si128((const __m128i*)(tm+8)),T3=_mm_load_si128((const __m128i*)(tm+12));
        __m128i acc=_mm_setzero_si128();
        for(size_t j=0;j<m;++j){const __m128i b=_mm_set1_epi32(tm[j]); // one byte per place: all ones where that click is later than click j
         acc=_mm_sub_epi8(acc,_mm_packs_epi16(_mm_packs_epi32(_mm_cmpgt_epi32(T0,b),_mm_cmpgt_epi32(T1,b)),_mm_packs_epi32(_mm_cmpgt_epi32(T2,b),_mm_cmpgt_epi32(T3,b))));}
        _mm_store_si128((__m128i*)rnk,acc);memset(taken,0,16);
        for(size_t i=0;i<m;++i){const unsigned r=rnk[i];keys[r+taken[r]++]=((C[i].key>>16)<<32)|i;}}
       // Clicks of one moment: the larger ID text first, so that the last one has the smallest.
       for(size_t i=1;i<m;++i)if(__builtin_expect((keys[i]>>32)==(keys[i-1]>>32),0)){
        const auto text_order=[&](uint64_t key){return order36(dn::value_of(ctr_of((uint32_t)key),dense.kc));};
        const uint64_t key=keys[i];size_t j=i;
        while(j>0&&(keys[j-1]>>32)==(key>>32)&&text_order(keys[j-1])<text_order(key)){keys[j]=keys[j-1];--j;}
        keys[j]=key;
       }
       bool dup=false;uint16_t nextMask[16];
       for(size_t i=0;i<16;++i){cts[i]=INT32_MAX;nextMask[i]=0;}
       memset(ccamp,0,sizeof ccamp); // (count_assists reads all sixteen places)
       for(size_t i=0;i<m;++i){const FClick& x=C[(uint32_t)keys[i]];const uint16_t c=(uint16_t)x.key;
        cts[i]=(int32_t)((int64_t)(keys[i]>>32)-WEEK);ccamp[i]=c;credit[i+1]=c; // times from the window start, like session times
        const uint8_t p=lastOf[c];if(p!=0xFF){nextMask[p]|=(uint16_t)(1u<<i);dup=true;}
        lastOf[c]=(uint8_t)i;}
       for(size_t i=0;i<m;++i)lastOf[ccamp[i]]=0xFF;
       credit[0]=organic; // read when no click is in the window
       dupMask[16]=0;
       if(dup)for(int a=15;a>=0;--a)dupMask[a]=dupMask[a+1]|nextMask[a]; // bit i: click i has an earlier click of its campaign at position a or later
       else memset(dupMask,0,sizeof dupMask);
       nvec=(m+3)/4;
      }
      const uint64_t* LK=nullptr; // large sets: the clicks' keys "time << 28 | position << 16 | campaign", in order
      if(mode==LARGE){
       uint64_t* const K=clickKeys3.get(m+1);LK=K;
       uint64_t all=0;
       if(m<=largeMax){const FClick* q=C;const FClick* const qe=q+m;uint64_t* o=K;uint64_t at=0;
        for(;q<qe;++q,++o,at+=0x10000u){const uint64_t k=q->key;all|=k;*o=((k>>16)<<28)|at|(uint16_t)k;__asm__("");}} // (the empty statement keeps the loop scalar)
       if(__builtin_expect(m>largeMax||(all>>48)!=0,0)){if(all>>48)mode=LARGEOLD;goto large_by_text;} // too many for a 12-bit position: the old order, keys made below (a time beyond 32 bits cannot be in a window this short, but would not fit the key)
       if(m>1){
        click_key_sort28(K,m,clickKeys2.get(m),sortCnt);
        // Clicks of one moment: the larger ID text first, so that the last one has the smallest.
        uint64_t prev=K[0]>>28;
        for(size_t i=1;i<m;++i){const uint64_t t=K[i]>>28;const bool same=t==prev;prev=t;
         if(__builtin_expect(same,0)){
          const auto text_order=[&](uint64_t key){return order36(dn::value_of(ctr_of((uint32_t)(key>>16)&0xFFFu),dense.kc));};
          const uint64_t key=K[i];size_t j=i;
          while(j>0&&(K[j-1]>>28)==t&&text_order(K[j-1])<text_order(key)){K[j]=K[j-1];--j;}
          K[j]=key;
         }}
       }
      }
      else if(mode==LARGEOLD&&m>1){
       large_by_text:;
       // Order by time through 8-byte keys (time, position), which sort faster than the clicks
       // themselves; clicks of one moment are then put in text order, and the clicks are copied out
       // in the final order.
       uint64_t* const keys=clickKeys.get(m);
       const bool wide=(C[0].key>>16)>>32!=0||(uint64_t)(window_end-clickStart)>>32!=0; // times that do not fit 32 bits: sort the clicks themselves
       if(wide||m>4096||(mode==LARGE&&m>largeMax)){
        if(direct){FClick* const G=fastC.get(m+1);for(size_t i=0;i<m;++i){G[i]=C[i];G[i].present=(uint32_t)(cf+i);}C=G;direct=false;} // sorted in a copy that carries the counters
        const auto before=[kc=dense.kc](const FClick& a,const FClick& b){const uint64_t ta=a.key>>16,tb=b.key>>16;
         return ta!=tb?ta<tb:order36(dn::value_of(a.present,kc))>order36(dn::value_of(b.present,kc));};
        // The same order through keys "time << 24 | position" and counting passes: the sort above is n log n
        // compares, and the biggest users have 50,000 clicks and more (SS_CLICKSORT=old: always that sort).
        uint64_t allT=0;for(size_t i=0;i<m;++i)allT|=C[i].key>>16;
        if(clickSortOld||wide||m>=(size_t(1)<<24)||(allT>>40)!=0)sort(C,C+m,before);
        else{
         uint64_t* K=clickKeys.get(m+1);
         for(size_t i=0;i<m;++i)K[i]=((C[i].key>>16)<<24)|i;
         K=time_pos_sort(K,clickKeys2.get(m+1),m,bigHist.data());
         const auto ord=[&](uint64_t key){return order36(dn::value_of(C[key&0xFFFFFFu].present,dense.kc));};
         for(size_t r0=0;r0<m;){ // clicks of one moment: the larger ID text first
          const uint64_t t=K[r0]>>24;size_t r1=r0+1;
          while(r1<m&&(K[r1]>>24)==t)++r1;
          if(__builtin_expect(r1-r0>1,0)){
           if(r1-r0==2){if(ord(K[r0])<ord(K[r0+1]))swap(K[r0],K[r0+1]);}
           else sort(K+r0,K+r1,[&](uint64_t x,uint64_t y){return ord(x)>ord(y);});
          }
          r0=r1;
         }
         FClick* const S=fastC2.get(m+1);
         for(size_t i=0;i<m;++i)S[i]=C[K[i]&0xFFFFFFu];
         C=S;
        }
       }
       else{
        for(size_t i=0;i<m;++i)keys[i]=((C[i].key>>16)<<32)|i;
        click_key_sort(keys,m,clickKeys2.get(m),sortCnt);
        for(size_t i=1;i<m;++i)if(__builtin_expect((keys[i]>>32)==(keys[i-1]>>32),0)){
         const auto text_order=[&](uint64_t key){return order36(dn::value_of(ctr_of((uint32_t)key),dense.kc));};
         const uint64_t key=keys[i];size_t j=i;
         while(j>0&&(keys[j-1]>>32)==(key>>32)&&text_order(keys[j-1])<text_order(key)){keys[j]=keys[j-1];--j;}
         keys[j]=key;
        }
        FClick* const S=fastC2.get(m+1);
        for(size_t i=0;i<m;++i)S[i]=C[(uint32_t)keys[i]];
        C=S;
       }
       if(mode==LARGE){uint64_t* const K=clickKeys3.get(m+1);for(size_t i=0;i<m;++i)K[i]=((C[i].key&~0xFFFFull)<<12)|(C[i].key&0xFFFFu);LK=K;} // (more than 4096 clicks)
      }
      char tmp[16];const NameRef nm=person_name(person,tmp);
      const FClick *add=C,*expire=C,*const ce=C+m;size_t na=0;
      const uint64_t *add8=LK,*exp8=LK,*const ce8=LK+(mode==LARGE?m:0);
      int currentday=-1;uint64_t dsess=0,drev=0;uint32_t rowStart=0;size_t pi=0;
      RowOut* const R=rowsOut.data();size_t nr=0;
      const auto close_row=[&](uint32_t evs) __attribute__((always_inline)) { // the finished row with its top category; written out after the person's last session
       RowOut& o=R[nr++];o.day=currentday;o.sess=(uint32_t)dsess;o.evs=evs;o.rev=drev;o.best=0;o.top=0;
       if(__builtin_expect(!touched.empty(),0)){
        uint16_t top=0;uint64_t best=0;
        for(uint16_t c:touched){if(catsum[c]>best||(catsum[c]==best&&catsum[c]>0&&catnames[c]<catnames[top])){top=c;best=catsum[c];}catsum[c]=0;}
        touched.clear();o.best=best;o.top=top;
       }};
      const auto count_assists=[&](int day) __attribute__((always_inline)) { // the row's byte counters into the totals
       if(!(cntLo|cntHi))return;
       uint64_t* const arow=assists+(size_t)day*ncamp;
       if(assistLoop){for(size_t i=0;i<m;++i)arow[ccamp[i]]+=(uint8_t)((i<8?cntLo:cntHi)>>(8*(i&7)));}
       else{ // eight at a time without a loop: the places after the last click hold campaign 0 and a zero count
        const uint64_t lo=cntLo;
        arow[ccamp[0]]+=(uint8_t)lo;arow[ccamp[1]]+=(uint8_t)(lo>>8);arow[ccamp[2]]+=(uint8_t)(lo>>16);arow[ccamp[3]]+=(uint8_t)(lo>>24);
        arow[ccamp[4]]+=(uint8_t)(lo>>32);arow[ccamp[5]]+=(uint8_t)(lo>>40);arow[ccamp[6]]+=(uint8_t)(lo>>48);arow[ccamp[7]]+=(uint8_t)(lo>>56);
        if(const uint64_t hi=cntHi){
         arow[ccamp[8]]+=(uint8_t)hi;arow[ccamp[9]]+=(uint8_t)(hi>>8);arow[ccamp[10]]+=(uint8_t)(hi>>16);arow[ccamp[11]]+=(uint8_t)(hi>>24);
         arow[ccamp[12]]+=(uint8_t)(hi>>32);arow[ccamp[13]]+=(uint8_t)(hi>>40);arow[ccamp[14]]+=(uint8_t)(hi>>48);arow[ccamp[15]]+=(uint8_t)(hi>>56);}
       }
       cntLo=0;cntHi=0;};
      // Large sets: the sessions [since, sn) of the row of `day` for every campaign in the window.
      const auto flush_window=[&](int day,uint32_t sn) __attribute__((always_inline)) {
       uint64_t* const arow=assists+(size_t)day*ncamp;
       for(const uint64_t* q=exp8;q<add8;++q){const uint16_t c=(uint16_t)*q;arow[c]+=sn-since[c];since[c]=sn;}};
      // The sessions, in one copy of the loop per kind of click set (M is a constant in each).
      uint64_t dayEnd=0;Totals* totRow=tot;uint64_t* assistRow=assists; // the row's day ends before dayEnd; its totals
      const auto run_sessions=[&](const int M) __attribute__((always_inline)) {
      for(size_t sn=0;sn<ns;++sn){
       const uint64_t entry=SL[sn],ts=entry>>24;const uint32_t ord=(uint32_t)(entry&0xFFFFFFu);
       if(ts>=dayEnd){ // the sessions come in time order, so a session is in a new day when it is past the end of the row's day
        const int d=(int)((ts+dayOff)/(uint64_t)DAY);
        if(currentday>=0){
         if(M==SMALL)count_assists(currentday);
         else if(M==LARGE)flush_window(currentday,(uint32_t)sn);
         close_row(ord-rowStart);dsess=0;drev=0;
        }
        currentday=d;rowStart=ord;dayEnd=(uint64_t)(d+1)*(uint64_t)DAY-dayOff;
        totRow=tot+(size_t)d*ncamp;assistRow=assists+(size_t)d*ncamp;
       }
       ++dsess;
       uint16_t credited=organic;
       if(M==SMALL){
        // In the window: start - seven days <= click time <= start.
        const __m128i vlo=_mm_set1_epi32((int)((int64_t)ts-WEEK)),vhi=_mm_set1_epi32((int)ts);
        uint32_t out=0;
        for(size_t v=0;v<nvec;++v){const __m128i x=_mm_load_si128((const __m128i*)(cts+4*v));
         out|=(uint32_t)_mm_movemask_ps(_mm_castsi128_ps(_mm_or_si128(_mm_cmplt_epi32(x,vlo),_mm_cmpgt_epi32(x,vhi))))<<(4*v);}
        const uint32_t in=~out&((1u<<(4*nvec))-1);
        credited=credit[31-__builtin_clz((in<<1)|1u)]; // the last click in the window, or entry 0
        const unsigned wfirst=(unsigned)__builtin_ctz(in|0x10000u); // the window's first place; 16: nothing in the window
        const uint32_t firsts=smallOld?in&~(uint32_t)dupMask[wfirst]
         :in&(uint32_t)_mm_movemask_epi8(_mm_cmpgt_epi8(_mm_load_si128((const __m128i*)(const void*)PLACE16[wfirst].data()),_mm_load_si128((const __m128i*)prevPlace))); // no click of its campaign before it in the window
        cntLo+=SPREAD8[firsts&255];cntHi+=SPREAD8[(firsts>>8)&255];
       }
       else if(M==LARGE){
        const uint64_t addLim=(ts+(uint64_t)WEEK+1)<<28,expLim=ts<<28;const uint32_t s32=(uint32_t)sn;
        uint64_t* const arow=assistRow;
        // (masks, not ?: - first / last click of a campaign in the window is as good as random)
        while(add8<ce8&&*add8<addLim){
         const uint16_t c=(uint16_t)*add8;const uint32_t f=fr[c],s=since[c];uint32_t first=0u-(uint32_t)(f==0);
         __asm__("":"+r"(first)); // (hidden from the compiler, which otherwise turns the mask back into a branch)
         fr[c]=f+1;since[c]=s^((s^s32)&first);++add8;}
        while(exp8<add8&&*exp8<expLim){
         const uint16_t c=(uint16_t)*exp8;const uint32_t f=fr[c]-1;
         fr[c]=f;arow[c]+=(s32-since[c])&(0u-(uint32_t)(f==0));++exp8;} // (a campaign that came and went with this session: nothing)
        if(add8>exp8)credited=(uint16_t)add8[-1];
       }
       else if(M==LARGEOLD){
        const uint64_t addLim=(ts+(uint64_t)WEEK+1)<<16,expLim=ts<<16;
        // Whether a click is the first (last) of its campaign in the window is as good as random, so the
        // list of active campaigns is kept without a branch on it: a write that must not happen goes to the
        // free place after the list (act has one place more than there are campaigns) or rewrites what is there.
        while(add<ce&&add->key<addLim){
         const uint16_t c=(uint16_t)add->key;const uint32_t f=fr[c],isNew=f==0,p=pos[c];
         fr[c]=f+1;act[na]=c;pos[c]=p^((p^(uint32_t)na)&(0u-isNew));na+=isNew;++add;} // (masks, not ?: - the compiler makes branches of those)
        while(expire<add&&expire->key<expLim){
         const uint16_t c=(uint16_t)expire->key;const uint32_t f=fr[c]-1,gone=f==0;
         fr[c]=f;na-=gone;
         const uint16_t back=act[na];const uint32_t at=(uint32_t)na^(((uint32_t)na^pos[c])&(0u-gone)),pb=pos[back];
         act[at]=back;pos[back]=pb^((pb^at)&(0u-gone));++expire;}
        if(add>expire)credited=(uint16_t)add[-1].key;
        uint64_t* const arow=assistRow;
        #pragma GCC unroll 4
        for(size_t q=0;q<na;++q)++arow[act[q]];
       }
       Totals* const cur=totRow+credited;cur->sessions++;
       if((pur[pi]>>40)==sn){ // the list ends with an all-ones entry, which is no session's
        cur->purchasing_sessions++;
        do{const uint64_t price=pur[pi]&EV_PRICE;const uint16_t cat=(uint16_t)(pur[pi]>>24);
         drev+=price;cur->revenue+=price;
         if(catsum[cat]==0)touched.push_back(cat);
         catsum[cat]+=price;
        }while((pur[++pi]>>40)==sn);
       }
      }
      if(M==SMALL)count_assists(currentday);
      else if(M==LARGE){flush_window(currentday,(uint32_t)ns);for(const uint64_t* q=exp8;q<add8;++q)fr[(uint16_t)*q]=0;}
      };
      switch(mode){case NONE:run_sessions(NONE);break;case SMALL:run_sessions(SMALL);break;case LARGE:run_sessions(LARGE);break;default:run_sessions(LARGEOLD);}
      close_row((uint32_t)n-rowStart);
      if(__builtin_expect(rowsOld,0))put_rows_old(nm,R,nr);else put_rows(nm,R,nr);
      for(size_t q=0;q<na;++q)fr[act[q]]=0;
      num_sessions+=ns;
      if(nrows>=FLUSH)flush_rows();
      ++rdFastIdx;rdFastEv+=n;rdFastCl+=m;
     }
     else{
      if(why==6&&!severalOld&&marked_index(ix,ec,cc,ef,el,cf,cl,stored))continue; // several persons, no general-lane row: finished here as well
      for(size_t c=ef;c<el;++c){const ESmall& sl=ev_at(c);const uint64_t k=sl.k;if(k<PR_KBIAS)continue;const uint32_t d=ucode(sl)^code;
       if(d<8){hand_event(myEv,pool,k,dBase+(uint32_t)((ix<<dn::ALIAS_BITS)|(d^((stored>>(3*d))&7))),sl.cat);++rdHandEv;++rdWhy[why];}}
      for(size_t c=cf;c<cl;++c){const CSlot& sl=denseCl[c];const uint32_t d=sl.user^code;
       if(sl.present&&d<8){hand_click(myCl,pool,txt,sl.key,dBase+(uint32_t)((ix<<dn::ALIAS_BITS)|(d^((stored>>(3*d))&7))),(uint32_t)c);++rdHandCl;}}
     }
    }
    thr_chunk_out();
   }
   // ---- DENSE stage, part 3: the rows of the deferred indexes, in one sweep. ----
   #pragma omp for schedule(dynamic,1) nowait
   for(size_t lo=min<uint64_t>(deferLoE,deferHiE);lo<deferHiE;lo+=ECH){
    const size_t hi=min<size_t>(deferHiE,lo+ECH);
    for(size_t c=lo;c<hi;++c){const ESmall& sl=ev_at(c);const uint64_t k=sl.k;if(k<PR_KBIAS)continue;const uint32_t u=ucode(sl);
     if(is_dense_code(u)){const uint32_t d=u&~DENSE_USER;
      if(info[d>>dn::ALIAS_BITS].state.load(memory_order_relaxed)==DSTATE_DEFER){hand_event(myEv,pool,k,dBase+(d^((comp[d>>3].load(memory_order_relaxed)>>(3*(d&7)))&7)),sl.cat);++rdHandEv;++rdWhy[8];}}}
   }
   #pragma omp for schedule(dynamic,1)
   for(size_t lo=min<uint64_t>(deferLoC,deferHiC);lo<deferHiC;lo+=CCH){
    const size_t hi=min<size_t>(deferHiC,lo+CCH);
    for(size_t c=lo;c<hi;++c){const CSlot& sl=denseCl[c];const uint32_t u=sl.user;
     if(sl.present&&(u&DENSE_USER)){const uint32_t d=u&~DENSE_USER;
      if(info[d>>dn::ALIAS_BITS].state.load(memory_order_relaxed)==DSTATE_DEFER){hand_click(myCl,pool,txt,sl.key,dBase+(d^((comp[d>>3].load(memory_order_relaxed)>>(3*(d&7)))&7)),(uint32_t)c);++rdHandCl;}}}
   }
   if(stats){ // every thread closes its DENSE record before one of them prints the line
    thr_stage_out();
    #pragma omp barrier
   }
   // The general lists are complete now: size the buffers of the rest of this stage by them.
   #pragma omp single
   {
    const double thrSg0=stats?now_s():0;
    if(slots){
     maxEvPerson=0;maxClPerson=0;
     for(size_t up=0;up<UPn;++up){size_t ne=0,nc=0;for(int t=0;t<T;++t){ne+=surv[t].ev[up].size();nc+=surv[t].cl[up].size();}
      maxEvPerson=max(maxEvPerson,ne);maxClPerson=max(maxClPerson,nc);}
     size_pools();
     if(stats){thrSingle=now_s()-thrSg0;thr_line("dense",dstat,thrR0,thrR0-thrS0,thrSingle);}
     __mark("DENSE");
    }
    if(stats)thrA0=now_s();
   }
  }
  if(stats){cur=&astat[tid];cur->in=now_s();cur->cIn=thr_cpu_s();} // from here on: the AGGREGATE stage
  bufA.reserve(maxEvPerson);bufB.reserve(maxEvPerson);bufC.reserve(maxClPerson);bufD.reserve(maxClPerson);
  if(cellPass)cellBuf.reserve(maxEvPerson+2);
  Cell* const cells=cellBuf.p;
  #pragma omp for schedule(dynamic,1)
  for(size_t up=0;up<UPn;++up){
   size_t nE=0,nC=0;
   for(int t=0;t<T;++t){nE+=surv[t].ev[up].size();nC+=surv[t].cl[up].size();}
   if(!nE)continue;
   thr_chunk_in();thr_chunk_out();
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
   thr_chunk_out();
  }
  // The loop above ended with a barrier: every thread's totals are final. (A failed write ends the run from
  // inside the region, as a failed write of a user_daily file does.)
  // The totals are added up by all threads (each cell by one thread), then one of them builds and writes the file.
  #pragma omp for schedule(static)
  for(size_t b=0;b<(campEarly?(ncells+MERGE_BLOCK-1)/MERGE_BLOCK:0);++b)merge_totals(b*MERGE_BLOCK,min(ncells,(b+1)*MERGE_BLOCK));
  #pragma omp single nowait
  {if(campEarly){__fine("campaign-begin");write_campaigns();__fine("campaign-end");}}
  const double thrE0=stats?now_s():0;
  // Every row is in the output buffers now (names are copied there): nothing points into an input file.
  #pragma omp for schedule(dynamic,1) nowait // whoever is free takes a piece: the thread with the campaign file comes late
  for(size_t i=0;i<unmapLast.size();++i)munmap(unmapLast[i].first,unmapLast[i].second);
  flush_rows();
  if(tid==0&&!file.is_open())open_file(); // the folder always holds at least one file, even with no rows
  if(file.is_open())file.close();
  if(stats){thr_stage_out();cur->endWall=cur->out-thrE0;}
 }
 if(!unmapLastBg.empty())thread([v=std::move(unmapLastBg)]{const size_t step=size_t(16)<<20;for(const auto& r:v)for(size_t at=0;at<r.second;at+=step)munmap(r.first+at,min(step,r.second-at));}).detach(); // SS_UNMAP=bg
 if(stats)thr_line("aggregate",astat,thrA0?thrA0:thrR0,0,0);
 if(earlyOut)for(size_t t=0;t<earlyOut->fd.size();++t)if(earlyOut->fd[t]>=0){ // a file no worker wrote to: it must not stay behind empty
  ::close(earlyOut->fd[t]);earlyOut->fd[t]=-1;char name[32];snprintf(name,sizeof name,"part-%05d.parquet",(int)t);::unlink((output/"user_daily"/name).c_str());}
 __fine("region-end");
 if(!campEarly)write_campaigns();
 if(earlyOut&&earlyOut->campFd>=0){::close(earlyOut->campFd);earlyOut->campFd=-1;} // SS_OUTOPEN=early: the campaign file is written by write_campaigns() through its own descriptor
__mark("AGGREGATE");
 if(slots&&stats){
  fprintf(stderr,"STATS dense reader: slots %llu events %llu clicks, fast lane %llu indexes %llu events %llu clicks, handed over %llu events %llu clicks, deferred indexes %llu\n",
   (unsigned long long)rdSlotEv,(unsigned long long)rdSlotCl,(unsigned long long)rdFastIdx,(unsigned long long)rdFastEv,(unsigned long long)rdFastCl,(unsigned long long)rdHandEv,(unsigned long long)rdHandCl,(unsigned long long)rdDeferIdx);
  fprintf(stderr,"STATS marked indexes finished by the index pass: %llu indexes %llu persons %llu events %llu clicks, %llu general-lane rows among them, %llu events taken out of line and merged back, %llu events fully sorted; %zu general-lane rows of dense users noted%s\n",
   (unsigned long long)rdMarkIdx,(unsigned long long)rdMarkPersons,(unsigned long long)rdMarkEv,(unsigned long long)rdMarkCl,(unsigned long long)rdMarkRows,(unsigned long long)rdMarkPulled,(unsigned long long)rdMarkSorted,genRefs.size(),genRefsComplete?"":" (too many: notes dropped, marked indexes handed over)");
  fprintf(stderr,"STATS handed-over events: table users %llu linked person, %llu alias 8+, %llu complex index (unlinked ID), %llu other text, %llu fallback; dense users %llu general-lane mark, %llu several persons, %llu test switch, %llu spread out\n",
   (unsigned long long)rdWhy[0],(unsigned long long)rdWhy[1],(unsigned long long)rdWhy[2],(unsigned long long)rdWhy[3],(unsigned long long)rdWhy[4],(unsigned long long)rdWhy[5],(unsigned long long)rdWhy[6],(unsigned long long)rdWhy[7],(unsigned long long)rdWhy[8]);
  fprintf(stderr,"STATS person buckets: %zu of 2^%u table persons, %zu of 2^%u dense persons\n",nA,shA,UPn-nA,shB);
 }
 {
  const string& csv=campCsv;
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
 if(__cpuLines)__sysflt("EXITPREP");
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
