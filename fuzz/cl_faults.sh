#!/bin/bash
# usage: cl_faults.sh WORKDIR [LIST="1 2 ..."] -- injected faults in the click readers: each one is a one-line change of a
# copy of main.cpp, built (-O2) and run through clwin_run.sh (damaged click lines, new reading with its check mode
# against the old one) and the real dev set. A fault is SEEN when one of them fails. Prints one line per fault.
W=$1; LIST=${2:-1 2 3 4 5 6 7 11 12 13 14 15 16 17 18 19 51 52 53 54 55 56 57}; H=$(cd "$(dirname "$0")" && pwd); SRC=$H/../main.cpp
CMP=/home/claude/bb/starter-pyspark/starter-pyspark/scripts/compare.py
mkdir -p $W
fault(){ case $1 in
 1) F='if((wB.n&mB)|(L>>6))return false;'; T='if((L>>6))return false;'; N='newline inside the user_id part not noticed';;
 2) F='if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; T='if((errA|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; N='quote error of the user_id ignored';;
 3) F='f.b1=(unsigned)(bB-p)+z1;f.l1=e1-2*z1;'; T='f.b1=(unsigned)(bB-p)+z1;f.l1=e1-z1;'; N='quoted user_id one byte too long';;
 4) F='if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; T='if((errA|errB|err)!=0||!c)return false;'; N='a fifth field accepted';;
 5) F='pu.k0=h;pu.u=u;pu.up=up;pu.j=(uint32_t)j;pu.k1=(uint32_t)n;'; T='pu.k0=h;pu.u=u;pu.up=up;pu.j=(uint32_t)j;pu.k1=(uint32_t)n-1;'; N='waiting long user one byte short';;
 6) F='if((errA|errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; T='if((errB|err|(uint64_t)(_mm_popcnt_u64(c)^1))!=0)return false;'; N='quote error of the click_id ignored';;
 7) F='f.next=bC+L+1;'; T='f.next=bC+L+1+(L==63);'; N='next line off by one when the newline is the last byte of window C';;
 11) F='const __m512i by=_mm512_maddubs_epi16(nib,_mm512_set1_epi16(0x0110));'; T='const __m512i by=_mm512_maddubs_epi16(nib,_mm512_set1_epi16(0x1001));'; N='kernel: hex digit weights swapped';;
 12) F='const __m512i err=_mm512_or_si512(_mm512_ternarylogic_epi64(errA,errB,errC,0xFE),dR);'; T='const __m512i err=_mm512_or_si512(_mm512_or_si512(errA,errC),dR);'; N='kernel: quote error of the user_id ignored';;
 13) F='const __m512i z1=_mm512_and_si512(QB,one),l1=_mm512_sub_epi64(e1,_mm512_add_epi64(z1,z1));'; T='const __m512i z1=_mm512_and_si512(QB,one),l1=_mm512_sub_epi64(e1,z1);'; N='kernel: quoted user_id one byte too long';;
 14) F='const __mmask8 ok=_mm512_mask_cmplt_epu64_mask(tsk,trel,VG_C(c.wlen));'; T='const __mmask8 ok=tsk;'; N='kernel: clicks outside the window accepted';;
 15) F='_mm512_slli_epi64(_mm512_add_epi64(l1,one),24));'; T='_mm512_slli_epi64(l1,24));'; N='kernel: user pack length off by one';;
 16) F='const uint64_t bad_=(na&(ca^(ca-1)))|(nb&(cb^(cb-1)))|((e0|e1|e3)>>6); /* (ca == 0: ca ^ (ca - 1) is all ones, and e0 is 64 anyway) */ \'; T='const uint64_t bad_=(na&(ca^(ca-1)))|((e0|e1|e3)>>6); \'; N='kernel scan: newline inside the user part not flagged';;
 17) F='uint64_t h=cls?hl:hp;'; T='uint64_t h=hp;'; N='loop: long user asked for and added under the packed hash';;
 18) F='const __mmask8 drop=_kor_mask8(dropE,_kandn_mask8(ok,tsk));'; T='const __mmask8 drop=_kor_mask8(dropE,_knot_mask8(ok));'; N='kernel: a ts the kernel does not decide is dropped';;
 19) F='const __mmask8 sh=_mm512_mask_testn_epi64_mask(_mm512_mask_cmple_epu64_mask(let,n,c8),bad,bad);
  __m512i hi,lo;const __mmask8 lg=_kand_mask8(_mm512_mask_cmpeq_epi64_mask(let,l0,c33),hex32k_v5(W,K2,K3,K4,hi,lo));'; T='const __mmask8 sh=_mm512_mask_cmple_epu64_mask(let,n,c8);
  __m512i hi,lo;const __mmask8 lg=_kand_mask8(_mm512_mask_cmpeq_epi64_mask(let,l0,c33),hex32k_v5(W,K2,K3,K4,hi,lo));'; N='kernel: short click_id class without the character test';;
 20) F='const __mmask8 nz=_mm512_mask_test_epi64_mask(_mm512_mask_test_epi64_mask(sok,l0,l0),l1,l1); // well-formed, click_id and user_id not empty'; T='const __mmask8 nz=_mm512_mask_test_epi64_mask(sok,l0,l0);'; N='kernel: empty user_id accepted';;
 21) F='sok=_mm512_mask_cmplt_epu64_mask(sok,L,c64);                                    // the newline inside window C'; T=''; N='kernel: newline outside window C not refused';;
 51) F=' if(__builtin_expect((user&LOCAL_USER)!=0,0)){tl.pclLocal.push_back(r);return;}'; T=' if(__builtin_expect((user&LOCAL_USER)!=0,0)){return;}'; N='direct: clicks of fallback users lost';;
 52) F='    out->key=r.c.key;out->id=r.c.id;out->person=u;out->idlen=r.c.idlen;};'; T='    out->key=r.c.key;out->id=r.c.id;out->person=r.c.person&~LOCAL_USER;out->idlen=r.c.idlen;};'; N='direct: a fallback user handed on without its person number';;
 53) F='  if(clDirect)tl.pcl.finish();'; T=''; N='direct: the last fill lines are never written out';;
 54) F='    if(__builtin_expect(lp.cdirect,1))click_direct(tl,O_.ks[i_],(uint32_t)O_.kl[i_],O_.ckh[i_]|(uint16_t)ci_,g_); \'; T='    if(__builtin_expect(lp.cdirect,1))click_direct(tl,O_.ks[i_],(uint32_t)O_.kl[i_],O_.ckh[i_]|(uint16_t)(ci_^1),g_); \'; N='direct: the kernel loop writes the neighbour campaign';;
 55) F='     click_direct(tl,s0,l0,ckey,ucode);
     break;'; T='     click_direct(tl,s0,l0,ckey,0);
     break;'; N='direct: the line code writes user 0';;
 56) F='     if(b>=tls[t].pcl.nb||!tls[t].pcl.count(b))continue;'; T='     if(b>=tls[t].pcl.nb||!tls[t].pcl.count(b)||(t==1&&b==7))continue;'; N='direct: one range of one thread is never handed on';;
 57) F='    if(__builtin_expect(lp.cdirect,1))click_direct(tl,O_.ks[i_],(uint32_t)O_.kl[i_],O_.ckh[i_]|(uint16_t)ci_,g_); \'; T='    if(__builtin_expect(lp.cdirect,1))click_direct(tl,O_.ks[i_],(uint32_t)O_.kl[i_]-((uint32_t)O_.kl[i_]>9),O_.ckh[i_]|(uint16_t)ci_,g_); \'; N='direct: a long click_id one byte short (only its order in ties can show it)';;
 *) return 1;; esac; }
for n in $LIST; do
 fault $n || { echo "fault $n: unknown"; continue; }
 python3 - "$SRC" "$W/main_f$n.cpp" "$F" "$T" <<'PY' || { echo "fault $n: the line to change is not in main.cpp (once)"; continue; }
import sys
s=open(sys.argv[1]).read()
if s.count(sys.argv[3])!=1: sys.exit(1)
open(sys.argv[2],'w').write(s.replace(sys.argv[3],sys.argv[4]))
PY
 g++-12 -std=c++20 -O2 -DNDEBUG -march=x86-64 -fopenmp -static -o $W/core_f$n $W/main_f$n.cpp 2>/dev/null || { echo "fault $n: build failed"; continue; }
 r1=$(RATE=0.3 REFCORE=${REFCORE:-} $H/clwin_run.sh $W/core_f$n $W/cw$n /home/claude/bb/r5/dev "1" 2>&1 | grep -c "^FAIL")
 O=$W/o$n; mkdir -p $O $W/s$n
 NCPU=4 DATA_DIR=/home/claude/bb/r5/dev OUTPUT_DIR=$O SCRATCH_DIR=$W/s$n $W/core_f$n >/dev/null 2>&1; r2=$(python3 $CMP $O /home/claude/bb/r5/ref_dev/out 2>&1 | tail -1)
 rm -rf $O $W/s$n $W/cw$n $W/core_f$n $W/main_f$n.cpp
 if [ "$r1" != 0 ] || [ "$r2" != MATCH ]; then echo "fault $n SEEN ($N): damaged-line runs failing $r1, real dev $r2"; else echo "fault $n NOT SEEN ($N)"; fi
done
