#include "postgres.h"
#include <access/hash.h>
//Constants
#define uint64_1 ((uint64)0x01)
#define uint32_1 ((uint32)0x01)
#define MAX_BUCKET 12
#define MAX_ERROR  6
#define MAX_COUNT 1024*1024*128

//Function
#define popcntll __builtin_popcountll
#define popcnt __builtin_popcount
#define HMCODE_SIZE(_len)		(offsetof(hmcode, x) + sizeof(uint8)*(_len))
#define DatumGetHmcode(x)		((hmcode *) PG_DETOAST_DATUM(x))
#define PG_GETARG_HMCODE_P(x)	DatumGetHmcode(PG_GETARG_DATUM(x))
#define PG_RETURN_HMCODE_P(x)	PG_RETURN_POINTER(x)

#define make_hashfunc(type, BTYPE, casttype) \
PG_FUNCTION_INFO_V1(hash##type); \
Datum \
hash##type(PG_FUNCTION_ARGS) \
{ \
	return hash_uint32((casttype) PG_GETARG_##BTYPE(0)); \
} \
extern int no_such_variable

make_hashfunc(uint4, UINT32, uint32);

//Macro
#define HMCODE_MAX_LEN 1024
#define WORD_LENGTH 8
#define HMCODE_MAX_DIM HMCODE_MAX_LEN * WORD_LENGTH

typedef struct hmcode
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int16		len;			  /* length of uint8 vector */
	int16		dim;        /* dim of hmcode */
	uint8		x[FLEXIBLE_ARRAY_MEMBER];
}			hmcode;


static inline hmcode *
InitHmcode(int len, int dim)
{
	hmcode	   *result;
	int			size;

	size = HMCODE_SIZE(len);
	result = (hmcode *) palloc0(size);
	SET_VARSIZE(result, size);
  result->len = len;
	result->dim = dim;

	return result;
}
static
const int lookup [] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};

static
inline int match(uint8* P, uint8* Q, int codelb) {
  switch(codelb) {
    case 4: // 32 bit
      return popcnt(*(uint32*)P ^ *(uint32*)Q);
      break;
    case 8: // 64 bit
      return popcntll(((uint64*)P)[0] ^ ((uint64*)Q)[0]);
      break;
    case 16: // 128 bit
      return popcntll(((uint64*)P)[0] ^ ((uint64*)Q)[0]) \
          + popcntll(((uint64*)P)[1] ^ ((uint64*)Q)[1]);
      break;
    case 32: // 256 bit
      return popcntll(((uint64*)P)[0] ^ ((uint64*)Q)[0]) \
          + popcntll(((uint64*)P)[1] ^ ((uint64*)Q)[1]) \
          + popcntll(((uint64*)P)[2] ^ ((uint64*)Q)[2]) \
          + popcntll(((uint64*)P)[3] ^ ((uint64*)Q)[3]);
      break;
    case 64: // 512 bit
      return popcntll(((uint64*)P)[0] ^ ((uint64*)Q)[0]) \
          + popcntll(((uint64*)P)[1] ^ ((uint64*)Q)[1]) \
          + popcntll(((uint64*)P)[2] ^ ((uint64*)Q)[2]) \
          + popcntll(((uint64*)P)[3] ^ ((uint64*)Q)[3]) \
          + popcntll(((uint64*)P)[4] ^ ((uint64*)Q)[4]) \
          + popcntll(((uint64*)P)[5] ^ ((uint64*)Q)[5]) \
          + popcntll(((uint64*)P)[6] ^ ((uint64*)Q)[6]) \
          + popcntll(((uint64*)P)[7] ^ ((uint64*)Q)[7]);
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
