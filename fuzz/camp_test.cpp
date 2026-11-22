// Unit test of the campaign table (camp_find): the text between the BEGIN / END camp table marks of main.cpp against
// an unordered_map. Build and run from the repository root:
//   sed -n "/BEGIN camp table/,/END camp table/p" main.cpp > /tmp/camp_extract.h && g++ -std=c++20 -O2 -I/tmp -o /tmp/camp_test fuzz/camp_test.cpp && /tmp/camp_test
// -> CAMP_TEST_OK (3000 key sets: c_1..c_2000, random numbers, random texts of 0-7 bytes incl. NUL and quote bytes, up to
// 32k keys; per set every key and 20000 other texts: neighbours of keys, random numbers, random bytes).
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <random>
using namespace std;
#include "camp_extract.h"
static uint32_t look(const CampTable& T,const string& s){char buf[16]={0};memcpy(buf,s.data(),s.size());return camp_table_find(T.look,camp_key(buf,s.size()));}
int main(){
 mt19937_64 rng(12345);uint64_t tests=0,fails=0,builds=0,nobuild=0;size_t maxS=0;
 const char alpha[]="c_0123456789\0\0ab\"C,\n ";const size_t na=sizeof(alpha)-1;
 for(int round=0;round<3000;++round){
  unordered_map<string,uint32_t> ref;vector<string> ids;
  const int kind=round%6;size_t n=kind==0?2000:kind==1?(size_t)(rng()%5000):kind==2?(size_t)(rng()%70):kind==3?(size_t)(1<<(rng()%13)):kind==4?(size_t)(rng()%300):(size_t)(20000+rng()%13000);
  if(kind==5&&round%60!=5)n=rng()%900;
  for(size_t i=0;i<n;++i){
   string id;
   if(kind==0)id="c_"+to_string(i+1);
   else if(kind==1)id="c_"+to_string(rng()%100000);
   else{const size_t l=rng()%8;for(size_t j=0;j<l;++j)id.push_back(kind==4?(char)rng():alpha[rng()%na]);}
   if(ref.emplace(id,(uint32_t)ids.size()).second)ids.push_back(id);
  }
  vector<CampEnt> ks;for(size_t i=0;i<ids.size();++i){char buf[16]={0};memcpy(buf,ids[i].data(),ids[i].size());ks.push_back({camp_key(buf,ids[i].size()),i});}
  CampTable T;
  if(!T.build(ks)){++nobuild;if(T.look.lim!=0){++fails;printf("FAIL: lim set without a table\n");}continue;}
  ++builds;maxS=max(maxS,T.t.size());
  if(round<12||round%500==0)printf("round %d kind %d keys %zu slots %zu\n",round,kind,ids.size(),T.t.size());
  for(size_t i=0;i<ids.size();++i){++tests;if(look(T,ids[i])!=i){++fails;if(fails<10)printf("FAIL: known id\n");}}
  for(int q=0;q<20000;++q){
   string s;const int m=q%5;
   if(m==0&&!ids.empty()){s=ids[rng()%ids.size()];if(!s.empty()){const int w=rng()%4;if(w==0)s.pop_back();else if(w==1&&s.size()<7)s.push_back(alpha[rng()%na]);else if(w==2)s[rng()%s.size()]^=(char)(1<<(rng()%8));else if(s.size()<7)s.push_back('\0');}}
   else if(m==1)s="c_"+to_string(rng()%300000);
   else{const size_t l=rng()%8;for(size_t j=0;j<l;++j)s.push_back(m==2?(char)rng():alpha[rng()%na]);}
   if(s.size()>7)continue;
   const auto it=ref.find(s);const uint32_t want=it==ref.end()?UINT32_MAX:it->second;
   ++tests;if(look(T,s)!=want){++fails;if(fails<10)printf("FAIL: text of %zu bytes: got %u want %u\n",s.size(),look(T,s),want);}
  }
 }
 printf("builds %llu no-table %llu lookups %llu fails %llu max slots %zu\n",(unsigned long long)builds,(unsigned long long)nobuild,(unsigned long long)tests,(unsigned long long)fails,maxS);
 puts(fails?"CAMP_TEST_FAIL":"CAMP_TEST_OK");return fails!=0;
}
