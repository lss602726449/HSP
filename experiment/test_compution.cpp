#include<iostream>
#include<vector>
#include<algorithm>
#include <sys/time.h>
using namespace std;

#define popcntll __builtin_popcountll
#define popcnt __builtin_popcount


static
const int lookup [] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};

static
inline int match(uint8_t* P, uint8_t* Q, int codelb) {
  switch(codelb) {
    case 4: // 32 bit
      return popcnt(*(uint32_t*)P ^ *(uint32_t*)Q);
      break;
    case 8: // 64 bit
      return popcntll(((uint64_t*)P)[0] ^ ((uint64_t*)Q)[0]);
      break;
    case 16: // 128 bit
      return popcntll(((uint64_t*)P)[0] ^ ((uint64_t*)Q)[0]) \
          + popcntll(((uint64_t*)P)[1] ^ ((uint64_t*)Q)[1]);
      break;
    case 32: // 256 bit
      return popcntll(((uint64_t*)P)[0] ^ ((uint64_t*)Q)[0]) \
          + popcntll(((uint64_t*)P)[1] ^ ((uint64_t*)Q)[1]) \
          + popcntll(((uint64_t*)P)[2] ^ ((uint64_t*)Q)[2]) \
          + popcntll(((uint64_t*)P)[3] ^ ((uint64_t*)Q)[3]);
      break;
    case 64: // 512 bit
      return popcntll(((uint64_t*)P)[0] ^ ((uint64_t*)Q)[0]) \
          + popcntll(((uint64_t*)P)[1] ^ ((uint64_t*)Q)[1]) \
          + popcntll(((uint64_t*)P)[2] ^ ((uint64_t*)Q)[2]) \
          + popcntll(((uint64_t*)P)[3] ^ ((uint64_t*)Q)[3]) \
          + popcntll(((uint64_t*)P)[4] ^ ((uint64_t*)Q)[4]) \
          + popcntll(((uint64_t*)P)[5] ^ ((uint64_t*)Q)[5]) \
          + popcntll(((uint64_t*)P)[6] ^ ((uint64_t*)Q)[6]) \
          + popcntll(((uint64_t*)P)[7] ^ ((uint64_t*)Q)[7]);
      break;
    default:{
        int output = 0;
        for (int i=0; i < codelb; i++) 
            output+= lookup[P[i] ^ Q[i]];
        return output;
      }  
      break;
  }

  return -1;
}

bool cmp(int a, int b){
  return a<b;
}

int main(){
    int num = 1000000, len = 8;
    // uint8_t** arr = (uint8_t**)malloc(num*sizeof(uint8_t*));
    // for(int i=0; i<num; i++){
    //   arr[i] = (uint8_t*)malloc(num*sizeof(uint8_t));;
    // }
    // uint8_t arr[num][len];
    uint8_t* arr = new uint8_t[num*len];
    uint8_t query[len];
    // vector<int> dis(num);
    int* dis = new int[num];
    for(int i=0; i<num; i++){
        for(int j=0; j<len; j++){
            arr[i*len+j] = rand()%256;
        }
    }
    for(int j=0; j<len; j++){
        query[j] = rand()%256;
    }
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);
    for(int i=0; i<num; i++){
        dis[i] = match(arr+i*len, query, len);
    }
    gettimeofday(&t2, NULL);
    double compute_time = (t2.tv_sec-t1.tv_sec) * 1000000 + t2.tv_usec-t1.tv_usec;
    cout<<"compute cost:"<<compute_time/1000<<"ms"<<endl;
    gettimeofday(&t1, NULL);
    // sort(dis.begin(), dis.end());
    // sort(arr, arr+num);
    std::nth_element(arr, arr+9, arr+num, std::less<int>{});
    gettimeofday(&t2, NULL);
    double sort_time = (t2.tv_sec-t1.tv_sec) * 1000000 + t2.tv_usec-t1.tv_usec;
    cout<<"compute cost:"<<sort_time/1000<<"ms"<<endl;
}