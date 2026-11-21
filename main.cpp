// Shopping Sessions: exact CSV / identity / attribution core.
// No third-party C++ libraries, intermediate output is hex-encoded to preserve raw identifiers.
#include <algorithm>
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
static vector<pair<void*,size_t>> held_mmaps;
static void release_mmaps(){for(auto [p,n]:held_mmaps)munmap(p,n);held_mmaps.clear();}
template<class FN> static void read_lines(const fs::path& path,FN&& fn,bool hold=false){
 int fd=open(path.c_str(),O_RDONLY);
 if(fd<0)throw runtime_error("Cannot open "+path.string());
 struct stat st{};if(fstat(fd,&st)!=0){close(fd);throw runtime_error("Cannot stat "+path.string());}
 size_t len=st.st_size;
 if(len==0){close(fd);return;}
 void* base=mmap(nullptr,len,PROT_READ,MAP_PRIVATE,fd,0);
 close(fd);
 if(base==MAP_FAILED)throw runtime_error("Cannot map "+path.string());
 const char* p=(const char*)base,*end=p+len;
 const char* nl=(const char*)memchr(p,'\n',end-p);
 if(!nl){if(hold)held_mmaps.push_back({base,len});else munmap(base,len);return;}
 p=nl+1;
 while(p<end){const char* begin=p;
  nl=(const char*)memchr(p,'\n',end-p);
  if(nl){fn(string_view(begin,(size_t)(nl-begin)));p=nl+1;}
  else{fn(string_view(begin,(size_t)(end-begin)));break;}
 }
 if(hold)held_mmaps.push_back({base,len});else munmap(base,len);
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
using FastSet=unordered_set<string,TransparentHash,equal_to<>>;
struct IdGraph{
 FastMap<uint32_t> keys;
 vector<uint32_t> parent,sz;
 vector<string> names;
 uint32_t intern(string_view s){auto it=keys.find(s);if(it!=keys.end())return it->second;
  uint32_t n=parent.size(); keys.emplace(string(s),n);parent.push_back(n);sz.push_back(1);names.emplace_back(s);return n;
 }
 uint32_t find(uint32_t x){uint32_t y=x;while(parent[y]!=y)y=parent[y];while(parent[x]!=x){auto z=parent[x];parent[x]=y;x=z;}return y;}
 void link(string_view a,string_view b){uint32_t x=find(intern(a)),y=find(intern(b));if(x==y)return;
  if(sz[x]<sz[y])swap(x,y);parent[y]=x;sz[x]+=sz[y];if(names[y]<names[x])names[x]=names[y];
 }
};
struct EventRecord{int64_t ingest=0,ts=0;uint32_t user=0;int32_t price=0;uint16_t cat=0;bool purchase=false,valid=false;};
struct DenseEvents {
 struct Entry {EventRecord rec{};const char* ptr=nullptr;uint32_t len=0;bool assigned=false;};
 struct Slot {uint32_t hash=0;uint32_t index=0;};
 vector<Slot> slots;
 vector<Entry> entries;
 void reserve(size_t n){size_t cap=1;while(cap<n*1.35+8)cap*=2;slots.resize(cap);entries.reserve((size_t)(n*1.75));}
 static uint64_t mix(uint64_t x){x^=x>>32;x*=0xd6e8feb86659fd93ULL;x^=x>>32;x*=0xd6e8feb86659fd93ULL;return x^(x>>32);}
 static uint64_t hkey(string_view s){
   const unsigned char* p=(const unsigned char*)s.data();size_t n=s.size();
   uint64_t h=0x9e3779b97f4a7c15ULL ^ (n*0xbf58476d1ce4e5b9ULL);
   while(n>=8){uint64_t w;memcpy(&w,p,8);h=(h^w)*0xa0761d6478bd642fULL;p+=8;n-=8;}
   uint64_t tail=0;memcpy(&tail,p,n);h=mix(h^tail);
   return h?h:1;
 }
 Entry* slot(string_view id,uint64_t h){
  h |= 1ULL; // exact same home bucket before/after rehash; 0 denotes empty
  if((entries.size()+1)*10>slots.size()*8){auto old=std::move(slots);slots.resize(old.size()?old.size()*2:1024);
   size_t mask=slots.size()-1;for(auto const&e:old)if(e.hash){size_t j=e.hash&mask;while(slots[j].hash)j=(j+1)&mask;slots[j]=e;}}
  size_t mask=slots.size()-1,j=h&mask;
  for(;;){auto& e=slots[j];
   if(!e.hash){e.hash=(uint32_t)h|1u;e.index=entries.size();entries.emplace_back();entries.back().ptr=id.data();entries.back().len=(uint32_t)id.size();return &entries.back();}
   if(e.hash==((uint32_t)h|1u) && entries[e.index].len==id.size()&&memcmp(entries[e.index].ptr,id.data(),id.size())==0)return &entries[e.index];
   j=(j+1)&mask;
  }
 }
 size_t size() const{return entries.size();}
 void clear(){vector<Slot>().swap(slots);vector<Entry>().swap(entries);}
};
struct DenseSet {
 struct Entry {const char* ptr=nullptr;uint32_t len=0,hash=0;};
 vector<Entry> slots;size_t count=0;
 void reserve(size_t n){size_t cap=1;while(cap<n*1.35+8)cap*=2;slots.resize(cap);}
 bool insert(string_view id){
  uint32_t h=(uint32_t)DenseEvents::hkey(id)|1u;
  size_t mask=slots.size()-1,j=h&mask;
  while(slots[j].hash){auto &e=slots[j];
   if(e.hash==h&&e.len==id.size()&&memcmp(e.ptr,id.data(),id.size())==0)return false;
   j=(j+1)&mask;
  }
  if((count+1)*10>slots.size()*8){auto old=std::move(slots);slots.resize(old.size()?old.size()*2:1024);count=0;
   for(const auto&e:old)if(e.hash)place(string_view(e.ptr,e.len),e.hash);
   mask=slots.size()-1;j=h&mask;while(slots[j].hash)j=(j+1)&mask;
  }
  slots[j]={id.data(),(uint32_t)id.size(),h};++count;return true;
 }
 void place(string_view id,uint32_t h){size_t mask=slots.size()-1,j=h&mask;
   while(slots[j].hash)j=(j+1)&mask;
   slots[j]={id.data(),(uint32_t)id.size(),h};++count;
 }
 void clear(){vector<Entry> t;t.swap(slots);count=0;}
};
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
 IdGraph graph;
 for(const auto& p:csv_files(data/"links"))read_lines(p,[&](string_view line){string_view f[2];if(fields(line,2,f)&&!f[0].empty()&&!f[1].empty())graph.link(f[0],f[1]);});
 __mark("LINKS"); cerr<<"linked IDs: "<<graph.parent.size()<<"\n";
 FastMap<uint16_t> categories;vector<string> catnames;
 cat_id("unknown",categories,catnames);
 FastMap<uint16_t> products;
 read_lines(data/"products.csv",[&](string_view line){string_view f[2];if(!fields(line,2,f))throw runtime_error("Malformed clean products.csv");products.emplace(string(f[0]),cat_id(f[1],categories,catnames));});
 vector<Camp> campaigns;FastMap<uint16_t> camps;
 read_lines(data/"campaigns.csv",[&](string_view line){string_view f[2];if(!fields(line,2,f))throw runtime_error("Malformed clean campaigns.csv");if(campaigns.size()>=65534)throw runtime_error("Too many campaigns");camps.emplace(string(f[0]),(uint16_t)campaigns.size());campaigns.push_back({string(f[0]),string(f[1])});});
 uint16_t organic=(uint16_t)campaigns.size();campaigns.push_back({"organic","organic"});
 // Earliest parseable duplicate, including one that later fails validation.
 DenseEvents eventmap;
 const auto eventFiles=csv_files(data/"events");
 uint64_t eventBytes=0;for(const auto& p:eventFiles)eventBytes+=fs::file_size(p);
 eventmap.reserve((size_t)(eventBytes/65+1));
 for(const auto& p:eventFiles)read_lines(p,[&](string_view line){
  string_view f[7];if(!fields(line,7,f)||f[0].empty())return;
  auto key=f[0];auto h=DenseEvents::hkey(key);
  __builtin_prefetch(&eventmap.slots[h&(eventmap.slots.size()-1)],1,1);
  int64_t ingest=0;if(!parse_ts(f[3],ingest))return; // ingest is guaranteed to be valid F1
  auto it=eventmap.slot(key,h);
  if(it->assigned && it->rec.ingest<=ingest)return;
  EventRecord rec;rec.ingest=ingest;
  int64_t ts;
  if(!f[1].empty()&&!f[5].empty()&&parse_ts(f[2],ts)&&ts>=window_start&&ts<window_end&&ingest-ts<=DAY&&
    (f[4]=="view"||f[4]=="cart"||f[4]=="purchase")){
    int32_t price=0;bool purchase=f[4]=="purchase";
    if(!purchase||price_value(f[6],price)){
      rec.valid=true;rec.ts=ts;rec.user=graph.intern(f[1]);rec.price=price;rec.purchase=purchase;
      if(purchase){auto prod=products.find(f[5]);rec.cat=prod==products.end()?0:prod->second;}
    }
  }
  it->rec=rec;it->assigned=true;
 },true);
__mark("EVENT_READ"); vector<Event> events;events.reserve(eventmap.size());
 for(const auto& entry:eventmap.entries){const auto& e=entry.rec;if(e.valid)events.push_back({e.ts,graph.find(e.user),e.price,e.cat,e.purchase});}
 cerr<<"deduplicated events: "<<eventmap.size()<<", surviving: "<<events.size()<<"\n";
 eventmap.clear();release_mmaps();
__mark("EVENT_COPY_FREE"); // A click's duplicates are identical; the first parseable occurrence is enough.
 vector<Click> clicks;DenseSet seen;
 const auto clickFiles=csv_files(data/"clicks");
 uint64_t clickBytes=0;for(const auto& p:clickFiles)clickBytes+=fs::file_size(p);
 seen.reserve((size_t)(clickBytes/36+1));
 for(const auto& p:clickFiles)read_lines(p,[&](string_view line){
  string_view f[4];if(!fields(line,4,f)||f[0].empty())return;
  auto key=f[0];if(!seen.insert(key))return;
  int64_t ts=0;auto ci=camps.find(f[2]);
  if(f[1].empty()||ci==camps.end()||!parse_ts(f[3],ts)||ts<window_start-WEEK||ts>=window_end)return;
  clicks.push_back({ts,graph.find(graph.intern(f[1])),ci->second,key});
 },true);
 cerr<<"surviving clicks: "<<clicks.size()<<"\n";
 seen.clear(); // Preserve click ID mmap until after sorting tie-breaks.
 {decltype(products) empty;products.swap(empty);} {decltype(categories) empty;categories.swap(empty);}
 {decltype(graph.keys) empty;graph.keys.swap(empty);}
__mark("CLICK_READ"); // graph must stay alive for canonical names; no new users are interned after this point.
 __gnu_parallel::sort(events.begin(),events.end(),[](const Event& a,const Event& b){if(a.user!=b.user)return a.user<b.user;return a.ts<b.ts;});
 __gnu_parallel::sort(clicks.begin(),clicks.end(),[](const Click& a,const Click& b){
  if(a.user!=b.user)return a.user<b.user;if(a.ts!=b.ts)return a.ts<b.ts;
  return a.id>b.id; // last click at equal timestamp has lexicographically smallest ID
 });
 release_mmaps();
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