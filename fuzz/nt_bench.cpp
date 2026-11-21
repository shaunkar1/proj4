// Micro-benchmark for the bucket writes of the parse stage: N 32-byte records scattered into P buckets
// (pre-faulted, 64-byte aligned), with plain stores or with a per-bucket 64-byte staging line that is
// flushed with non-temporal stores. `work` dependent multiplies per record stand in for the parsing.
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
using namespace std;
struct Rec{uint64_t a,b,c,d;};
static inline uint64_t mix(uint64_t x){x^=x>>32;x*=0xd6e8feb86659fd93ULL;x^=x>>32;return x;}
__attribute__((target("avx2"))) static double run(int mode,size_t N,size_t P,int work,bool huge,uint64_t& sink){
 const size_t per=N/P*9/8+16,bytes=P*per*sizeof(Rec);
 char* mem=(char*)mmap(nullptr,bytes+(2<<20),PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,-1,0);
 char* al=(char*)(((uintptr_t)mem+(2<<20)-1)&~(uintptr_t)((2<<20)-1));
 if(huge)madvise(al,bytes,MADV_HUGEPAGE);
 memset(al,0,bytes); // pre-fault, as the pipeline does
 vector<Rec*> pos(P);for(size_t p=0;p<P;++p)pos[p]=(Rec*)(al+p*per*sizeof(Rec));
 vector<uint32_t> cnt(P,0);
 Rec* stage=(Rec*)aligned_alloc(64,P*2*sizeof(Rec));memset(stage,0,P*2*sizeof(Rec));
 auto t0=chrono::steady_clock::now();
 uint64_t x=12345,acc=0;
 for(size_t i=0;i<N;++i){
  x=mix(x+i);
  uint64_t w=x;for(int k=0;k<work;++k)w=w*0x9E3779B97F4A7C15ULL+k; // stand-in for parsing a line
  acc+=w;
  const size_t p=(size_t)(((x>>40)*P)>>24);
  const __m256i r=_mm256_set_epi64x((long long)w,(long long)x,(long long)i,(long long)p);
  if(mode==0){_mm256_storeu_si256((__m256i*)pos[p],r);++pos[p];}
  else{
   const uint32_t n=cnt[p]++;
   _mm256_store_si256((__m256i*)(stage+2*p+(n&1)),r);
   if(n&1){Rec* d=pos[p];_mm256_stream_si256((__m256i*)d,_mm256_load_si256((const __m256i*)(stage+2*p)));_mm256_stream_si256((__m256i*)(d+1),r);pos[p]=d+2;}
  }
 }
 if(mode==1){for(size_t p=0;p<P;++p)if(cnt[p]&1)*pos[p]=stage[2*p];_mm_sfence();}
 const double s=chrono::duration<double>(chrono::steady_clock::now()-t0).count();
 // read everything back once (what the dedup stage does), outside the timing
 for(size_t p=0;p<P;++p){const Rec* b=(const Rec*)(al+p*per*sizeof(Rec));acc+=b[0].a;}
 sink+=acc;free(stage);munmap(mem,bytes+(2<<20));return s;
}
int main(int argc,char** argv){
 const size_t N=argc>1?strtoull(argv[1],0,10):20000000;const int reps=argc>2?atoi(argv[2]):5;uint64_t sink=0;
 for(int huge=1;huge>=0;--huge)for(size_t P:{(size_t)1024,(size_t)2048})for(int work:{0,40}){
  double best[2]={1e9,1e9};
  for(int r=0;r<reps;++r)for(int mode=0;mode<2;++mode){double s=run(mode,N,P,work,huge,sink);if(s<best[mode])best[mode]=s;}
  printf("huge=%d buckets=%zu work=%2d: plain %.3f s, staged+NT %.3f s (min of %d)  NT/plain=%.2f\n",huge,P,work,best[0],best[1],reps,best[1]/best[0]);fflush(stdout);
 }
 return (int)(sink&1);
}
