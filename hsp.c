#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "stdint.h"
#include <math.h>

#include <hsp.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


static inline void 
CheckDims(hmcode* a, hmcode* b){
	if(a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("cannot process data with different  dimension")));
}

static inline void 
CheckElement(uint64 dimension){
	if(dimension > PG_UINT8_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("dimension value exceed other uint8 max")));
}

static inline void
CheckDim(int dim)
{
	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hmcode must have at least 1 dimension")));

	if (dim > HMCODE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hmcode cannot have more than %d dimensions", HMCODE_MAX_DIM)));
}

static inline void
CheckExpectedDim(int32 typmod, int len)
{
	if (typmod != -1 && typmod/WORD_LENGTH != len)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("expected %d dimensions, so length cannot be %d", typmod, len)));
}



// the name of the function should be lower case
PG_FUNCTION_INFO_V1(hmcode_in);
Datum
hmcode_in(PG_FUNCTION_ARGS)
{
    char	   *str = PG_GETARG_CSTRING(0);
	int		    typmod = PG_GETARG_INT32(2);
	int			i;
	uint8		x[HMCODE_MAX_LEN];
	int			dim = 0;
	int 		len = 0;
	char	   *pt;
	char	   *stringEnd;
	hmcode	   *result;
	unsigned long int dimension;
	
	if (*str != '{')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("error formed hmcode literal: \"%s\", must start with { ", str)));

	str++;
	pt = strtok(str, ",");
	stringEnd = pt;

	while (pt != NULL && *pt != '}')
	{
		if (len == HMCODE_MAX_LEN)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("hmcode length is no more than %d ", HMCODE_MAX_LEN)));

        dimension = strtoul(pt, &stringEnd, 10);
		CheckElement(dimension);
		x[len] = dimension;
		len++;
		if (stringEnd == pt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type hmcode: \"%s\"", pt)));

		if (*stringEnd != '\0' && *stringEnd != '}')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type hmcode: \"%s\"", pt)));

		pt = strtok(NULL, ",");
	}
	if (*stringEnd != '}')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed hmcode literal"),
				 errdetail("Unexpected end of input.")));

	if (stringEnd[1] != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed hmcode literal"),
				 errdetail("Junk after closing right brace.")));

	if (len < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hmcode length cannot lowwer than 1")));

	CheckExpectedDim(typmod, len);

	if(typmod <= 0){
		dim = len*WORD_LENGTH;
	}else{
		dim = typmod;
	}

	result = InitHmcode(len, dim);
	for (i = 0; i < len; i++)
		result->x[i] = x[i];

	PG_RETURN_POINTER(result);
}



/*
 * Convert internal representation to textual representation
 */
PG_FUNCTION_INFO_V1(hmcode_out);
Datum
hmcode_out(PG_FUNCTION_ARGS)
{
	hmcode	   *hc = PG_GETARG_HMCODE_P(0);
	StringInfoData buf;
	int			len = hc->len;
	int			i;
	char	   *result = palloc(4);

	initStringInfo(&buf);
			/* 3 digits, '\0' */
	appendStringInfoChar(&buf, '{');
	for (i = 0; i < len; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ",");
		sprintf(result, "%u", hc->x[i]);
		appendStringInfoString(&buf, result);
	}
	appendStringInfoChar(&buf, '}');

	PG_FREE_IF_COPY(hc, 0);
	PG_RETURN_CSTRING(buf.data);
}


/*
 * Convert type modifier
 */
PG_FUNCTION_INFO_V1(hmcode_typmod_in);
Datum
hmcode_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type modifier")));

	if (*tl % WORD_LENGTH != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type hmcode must be a multiple of %d",WORD_LENGTH)));

	if (*tl < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type hmcode must be at least 1")));

	if (*tl > HMCODE_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dimensions for type hmcode cannot exceed %d", HMCODE_MAX_DIM)));

	PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
PG_FUNCTION_INFO_V1(hmcode_recv);
Datum
hmcode_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	hmcode	   *result;
	int16		len;
	int16		dim;
	int			i;

	len = pq_getmsgint(buf, sizeof(int16));
	dim = pq_getmsgint(buf, sizeof(int16));

	CheckExpectedDim(typmod, dim);


	result = InitHmcode(len, dim);
	for (i = 0; i < len; i++)
		result->x[i] = pq_getmsgbyte(buf);

	PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
PG_FUNCTION_INFO_V1(hmcode_send);
Datum
hmcode_send(PG_FUNCTION_ARGS)
{
	hmcode	   *hmCode = PG_GETARG_HMCODE_P(0);
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);
	pq_sendint(&buf, hmCode->len, sizeof(int16));
	pq_sendint(&buf, hmCode->dim, sizeof(int16));
	for (i = 0; i < hmCode->len; i++)
		pq_sendbyte(&buf, hmCode->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

void
PrintHmcode(char *msg, hmcode * hmcode)
{
	StringInfoData buf;
	int			dim = hmcode->dim;
	int			i;
	char	   *result = palloc(4);		/* 3 digits, '\0' */

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '{');
	for (i = 0; i < dim; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ",");

		sprintf(result, "%u", hmcode->x[i]);
		appendStringInfoString(&buf, result);
	}
	appendStringInfoChar(&buf, '}');

	elog(INFO, "%s = %s", msg, buf.data);
}

/*
 * Convert hmcode to hmcode
 */
PG_FUNCTION_INFO_V1(hmcode_convert);
Datum
hmcode_convert(PG_FUNCTION_ARGS)
{
	hmcode	   *arg = PG_GETARG_HMCODE_P(0);
	int32		typmod = PG_GETARG_INT32(1);

	CheckExpectedDim(typmod, arg->len);

	PG_RETURN_POINTER(arg);
}

/*
 * Convert array to hamcode
 */
PG_FUNCTION_INFO_V1(array_to_hmcode);
Datum
array_to_hmcode(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	int			i;
	hmcode	   *result;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elemsp;
	bool	   *nullsp;
	int			nelemsp;
	int 		temp;

	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);

	if (typmod == -1)
		CheckDim(nelemsp);
	else
		CheckExpectedDim(typmod, nelemsp*WORD_LENGTH);

	result = InitHmcode(nelemsp,nelemsp*WORD_LENGTH);
	for (i = 0; i < nelemsp; i++)
	{
		if (nullsp[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("array must not containing NULLs")));
		if (ARR_ELEMTYPE(array) == INT4OID)
			temp = DatumGetInt32(elemsp[i]);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("unsupported array type")));
		CheckElement(temp);
		result->x[i] = (uint8)temp;
	}

	PG_RETURN_POINTER(result);
}



/*
 * Convert hmcode to int4[]
 */
PG_FUNCTION_INFO_V1(hmcode_to_array);
Datum
hmcode_to_array(PG_FUNCTION_ARGS)
{
	hmcode	   *hc = PG_GETARG_HMCODE_P(0);
	Datum	   *d;
	ArrayType  *result;
	int			i;

	d = (Datum *) palloc(sizeof(Datum) * hc->len);

	for (i = 0; i < hc->len; i++)
		d[i] = Int32GetDatum((int32)hc->x[i]);

	/* Use TYPALIGN_INT for int4 */
	result = construct_array(d, hc->len, INT4OID, sizeof(int32), true, 'i');

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(hamming_distance);
Datum
hamming_distance(PG_FUNCTION_ARGS)
{
	hmcode	   *a = PG_GETARG_HMCODE_P(0);
	hmcode	   *b = PG_GETARG_HMCODE_P(1);
	int 		distance = 0;

	CheckDims(a, b);

	distance  = match(a->x,b->x,a->len);

	PG_RETURN_INT32(distance);
}

static uint64
pg_atou(const char *s, int size)
{
	uint64 		result;
	bool		out_of_range = false;
	char	   *badp;

	if (s == NULL)
		elog(ERROR, "NULL pointer");
	if (*s == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for unsigned integer: \"%s\"",
						s)));

	if (strchr(s, '-'))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for unsigned integer: \"%s\"",
						s)));

	errno = 0;
	result = strtoul(s, &badp, 10);

	switch (size)
	{
		case sizeof(uint32):
			if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
				|| result > PG_UINT32_MAX
				#endif
				)
				out_of_range = true;
			break;
		case sizeof(uint16):
			if (errno == ERANGE || result > PG_UINT16_MAX)
				out_of_range = true;
			break;
		case sizeof(uint8):
			if (errno == ERANGE || result > PG_UINT8_MAX)
				out_of_range = true;
			break;
		default:
			elog(ERROR, "unsupported result size: %d", size);
	}

	if (out_of_range)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type uint%d", s, size)));

	while (*badp && isspace((unsigned char) *badp))
		badp++;

	if (*badp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for unsigned integer: \"%s\"",
						s)));

	return result;
}


PG_FUNCTION_INFO_V1(uint4_in);
Datum
uint4_in(PG_FUNCTION_ARGS)
{
	char	   *s = PG_GETARG_CSTRING(0);
	uint32 		result  = pg_atou(s, sizeof(uint32));
	PG_RETURN_UINT32(result);
}

PG_FUNCTION_INFO_V1(uint4_out);
Datum
uint4_out(PG_FUNCTION_ARGS)
{
	uint32		arg1 = PG_GETARG_UINT32(0);
	char	   *result = palloc(11);	/* 10 digits, '\0' */

	sprintf(result, "%u", arg1);
	PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(uint4_eq);
Datum
uint4_eq(PG_FUNCTION_ARGS)
{
	uint32		arg1 = PG_GETARG_UINT32(0);
	uint32		arg2 = PG_GETARG_UINT32(1);

	
	PG_RETURN_BOOL(arg1 == arg2);
}
PG_FUNCTION_INFO_V1(uint4_hamming);
Datum
uint4_hamming(PG_FUNCTION_ARGS)
{
	uint32		arg1 = PG_GETARG_UINT32(0);
	uint32		arg2 = PG_GETARG_UINT32(1);

	
	PG_RETURN_INT32(match((uint8*)&arg1,(uint8 *)&arg2, 4));
}


PG_FUNCTION_INFO_V1(hmcode_split);
Datum
hmcode_split(PG_FUNCTION_ARGS)
{
	hmcode	   *hc = PG_GETARG_HMCODE_P(0);
	int 		m = PG_GETARG_INT32(1);
	if(m<=0 || m>hc->dim)
		ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("bad parameter m")));
	ArrayType  *at;
	Datum	   *d = (Datum *) palloc(m * sizeof(Datum));
	uint32	   *result = palloc0(m * sizeof(uint32));	
	int 		dim = hc->dim;
	int 		b = ceil((double)dim/m);
	int 		mplus = dim - m * (b-1);
	if(b>32)
		ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("each partition max length is 32")));
	int 		nbits = 0;
  	int 		nbyte = 0;
	uint32 		temp = 0x0;
	uint32 		mask = b==32 ? PG_UINT32_MAX : ((uint32_1 << b) - uint32_1);

	// mplus     is the number of chunks with b bits
  	// (m-mplus) is the number of chunks with (b-1) bits

	for (int i=0; i<m; i++) {
		while (nbits < b) {
			temp |= ((uint32)hc->x[nbyte++] << nbits);
			nbits += 8;
    	}
		result[i] = temp & mask;
		temp = b==32 ? 0x0 : temp >> b; 
		nbits -= b;
		if (i == mplus-1) {
			b--;		/* b <= 31 */
			mask = ((uint32_1 << b) - uint32_1);
		}
	}
	for (int i = 0; i < m; i++)
		d[i] = Int64GetDatum((int64)result[i]);
	at = construct_array(d, m, INT8OID, sizeof(int64), true, 'd');//there will be some trick 
	PG_RETURN_POINTER(at);
}

PG_FUNCTION_INFO_V1(get_hmcode_dim);
Datum
get_hmcode_dim(PG_FUNCTION_ARGS)
{
	hmcode*		arg1 = PG_GETARG_HMCODE_P(0);
	PG_RETURN_INT32(arg1->dim);
}



PG_FUNCTION_INFO_V1(get_query_cand);
Datum
get_query_cand(PG_FUNCTION_ARGS)
{
	uint32		code = PG_GETARG_UINT32(0);
	int			b = PG_GETARG_INT32(1);
	int			s = PG_GETARG_INT32(2);
	
	ArrayType  *at;
	Datum	   *d;
	int			total = 1;
	int			count = 0;
	uint32	   *result; 
	uint32		bitstr;
	int 		power[100];
	int 		bit;
	for(int i=0; i<s; i++){
		total = (total*(b-i))/(i+1);
	}
	result = palloc0(total * sizeof(uint32));
	d = (Datum *) palloc(total * sizeof(Datum));

	bitstr = 0; 	       	// the bit-string with s number of 1s
    for (int i=0; i<s; i++)
        power[i] = i;			// power[i] stores the location of the i'th 1
    power[s] = b+1;	       		// used for stopping criterion (location of (s+1)th 1)

    bit = s-1;				// bit determines the 1 that should be moving to the left
    // we start from the left-most 1, and move it to the left until it touches another one

    while (true) {			// the loop for changing bitstr
        if (bit != -1) {
        	bitstr ^= (power[bit] == bit) ? (uint64_t)1 << power[bit] : (uint64_t)3 << (power[bit]-1);
          	power[bit]++;
          	bit--;
        } else { 
			// bit == -1
			result[count++] = code ^ bitstr; 
          	/* end of processing */
			while (++bit < s && power[bit] == power[bit+1]-1) {
            	bitstr ^= (uint64_t)1 << (power[bit]-1);
            	power[bit] = bit;
          	}
          	if (bit == s)
            	break;
        }
	}
	for (int i = 0; i < total; i++)
		d[i] = Int64GetDatum((int64)result[i]);
	at = construct_array(d, total, INT8OID, sizeof(int64), true, 'd');//there will be some trick 
	PG_RETURN_POINTER(at);
}

PG_FUNCTION_INFO_V1(get_statistics_array);
Datum
get_statistics_array(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elemsp;
	bool	   *nullsp;
	int			nelemsp;
	uint32_t 	temp;

	int		 	dim = PG_GETARG_INT32(1);
	int		 	m = PG_GETARG_INT32(2);
	int 		b = ceil((double)dim/m);
	ArrayType  *at;
	Datum	   *d;
	uint32 valuemask;
	uint32 bucketsize;
	int length;
	int* table;
	int max_error, max_bit;

	if(dim != m*b){
		b = b-1;
	}

	if (b > MAX_BUCKET) {
		max_bit = MAX_BUCKET;
	}else{
		max_bit = b;
	}

	bucketsize = pow(2, max_bit);
	valuemask = bucketsize-1;
	
	if (b > MAX_ERROR) {
		max_error = MAX_ERROR;
	} else {
		max_error = b;
	}

	length = max_error * bucketsize;
	d = (Datum *) palloc(length * sizeof(Datum));
	table = (int*)palloc(length * sizeof(int));
	memset(table,0,length*sizeof(int));
	
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);

	for (int i = 0; i < nelemsp; i++)
	{
		if (nullsp[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("array must not containing NULLs")));
		if (ARR_ELEMTYPE(array) == INT8OID)
			temp = DatumGetInt64(elemsp[i]);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("unsupported array type")));
		for (uint32_t j = 0; j < bucketsize; j++) {
			uint32_t err = __builtin_popcount((temp ^ j) & valuemask);
			if (err < max_error)
			table[j*max_error + err] += 1;
		}
	}

	for (int i = 0; i < length; i++)
		d[i] = Int32GetDatum(table[i]);
	at = construct_array(d, length, INT4OID, sizeof(int), true, 'i');
	PG_RETURN_POINTER(at);
}

int64 search(int* hist, uint32 index, uint32 valuemask, int max_error, int error){
	if (error < 0) {
		return 0;
	}    
	if (error>=max_error) {
		return MAX_COUNT;
	}
	return hist[((index&valuemask) * max_error) + error];
}

PG_FUNCTION_INFO_V1(get_slots);
Datum
get_slots(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elemsp;
	bool	   *nullsp;
	int			nelemsp;

	int		 	dim = PG_GETARG_INT32(2);
	int		 	m = PG_GETARG_INT32(3);
	int			t = PG_GETARG_INT32(4)+1;
	int 		b = ceil((double)dim/m);
	
	ArrayType  *at;
	Datum	   *d;
	uint32 bucketsize;
	uint32 valuemask;
	int length;
	int* hist;

	int pid;
	int** DistCost;
	int** DistPath;
	int part;
	int* slots;
	int err;
	int cumulativecost;

	// read the hist array
	int max_error, max_bit;
	if (b > MAX_BUCKET) {
		max_bit = MAX_BUCKET;
	}else{
		max_bit = b;
	}
	
	bucketsize = pow(2, max_bit);
	
	valuemask = bucketsize-1;
	if (b > MAX_ERROR) {
		max_error = MAX_ERROR;
	} else {
		max_error = b;
	}
	
	length = m * max_error * bucketsize;
	
	hist = (int*)palloc(length * sizeof(int32));
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);

	for (int i = 0; i < nelemsp; i++)
	{
		hist[i] = DatumGetInt32(elemsp[i]);
	}
	// read the query array
	array = PG_GETARG_ARRAYTYPE_P(1);
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);
	uint32* query_arr = (uint32*)palloc(m * sizeof(uint32));
	for (int i = 0; i < nelemsp; i++)
	{
		query_arr[i] = (uint32)DatumGetInt64(elemsp[i]);
	}
	 
	pid = 0;
	DistCost = (int**)palloc(m*sizeof(int*));
	DistPath = (int**)palloc(m*sizeof(int*));
	for(int i=0; i<m; i++){
		DistCost[i] = (int*)palloc((t+1)*sizeof(int));
		DistPath[i] = (int*)palloc((t+1)*sizeof(int));
	}
	cumulativecost = 0; 
  
	
	part = max_error * bucketsize;
	for (int dst = 0; dst <= t; dst++) {
		// elog(INFO, " cumulativecost %d ", (int)cumulativecost);
		cumulativecost += search(hist+0*part, query_arr[0], valuemask, max_error, dst-1);
		DistCost[0][dst] = cumulativecost;
		DistPath[0][dst] = dst;
	}

  	// Process Intermediate buckets.
	for (pid = 1; pid < m; ++pid) {
		for (int dst = 0; dst <= t; ++dst) {
			cumulativecost = search(hist+pid*part , query_arr[pid], valuemask, max_error, 0);
			DistCost[pid][dst] = DistCost[pid-1][dst] + search(hist+pid*part, query_arr[pid], valuemask, max_error, 0);
			DistPath[pid][dst] = 0;
			for (int err = 1; err <= t && dst - err >=0; err ++) {
				cumulativecost +=  search(hist+pid*part, query_arr[pid], valuemask, max_error, err-1);
				int current_cost = DistCost[pid-1][dst-err] + cumulativecost;
				if (DistCost[pid][dst] > current_cost) {
					DistCost[pid][dst] = current_cost;
					DistPath[pid][dst] = err;
				}
			}
		}
	}
	 
	slots = (int*)palloc(m * sizeof(int));
	// Backtrace the error path.
	
	err = t ;
	for (pid = m - 1; pid >= 0; --pid) {
		slots[pid] = DistPath[pid][err]-1;
		err = err - DistPath[pid][err];
	}

	// for (pid = 0; pid < m; pid ++) {
	//   elog(INFO, "\nDP pid %d-> ", pid);
	//   for (err = 0; err <= t; err ++) {
	//     elog(INFO, "(%d|%d) ", (int)DistCost[pid][err], DistPath[pid][err]);
	//   }
	//   elog(INFO, "\n");
	// }

	d = palloc(m*sizeof(Datum));
	for (int i = 0; i < m; i++){
		// elog(INFO, " slot %d %d", i, slots[i]);
		d[i] = Int32GetDatum(slots[i]);
	}
	at = construct_array(d, m, INT4OID, sizeof(int), true, 'i');
	PG_RETURN_POINTER(at);
}

PG_FUNCTION_INFO_V1(get_greedy);
Datum
get_greedy(PG_FUNCTION_ARGS)
{
	ArrayType  *array;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elemsp;
	bool	   *nullsp;
	int			nelemsp;

	int		 	dim = PG_GETARG_INT32(2);
	int		 	m = PG_GETARG_INT32(3);
	int			t = m * MAX_ERROR;
	int 		b = ceil((double)dim/m);
	
	ArrayType  *at;
	Datum	   *d;
	uint32 bucketsize;
	uint32 valuemask;
	int length;
	int* hist;

	int* greedy;
	int* allo;
	int part;

	// read the hist array
	int max_error, max_bit;
	if (b > MAX_BUCKET) {
		max_bit = MAX_BUCKET;
	}else{
		max_bit = b;
	}
	
	bucketsize = pow(2, max_bit);
	
	valuemask = bucketsize-1;
	if (b > MAX_ERROR) {
		max_error = MAX_ERROR;
	} else {
		max_error = b;
	}
	
	length = m * max_error * bucketsize;
	
	array = PG_GETARG_ARRAYTYPE_P(0);
	hist = (int*)palloc(length * sizeof(int32));
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);

	for (int i = 0; i < nelemsp; i++)
	{
		hist[i] = DatumGetInt32(elemsp[i]);
	}
	// read the query array
	array = PG_GETARG_ARRAYTYPE_P(1);
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("array must be 1-D")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
	deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, &nullsp, &nelemsp);
	uint32* query_arr = (uint32*)palloc(m * sizeof(uint32));
	for (int i = 0; i < nelemsp; i++)
	{
		query_arr[i] = (uint32)DatumGetInt64(elemsp[i]);
	}

	greedy = (int*)palloc(t*sizeof(int*));
	allo = (int*)palloc(m*sizeof(int*));

	for(int i=0; i<m; i++){
		allo[i] = 0;
	}
	
	part = max_error * bucketsize;
	for(int pid = 0; pid<t; pid++){
		int min_value = MAX_COUNT+1;
		int temp = 0;
		for(int i=0; i<m; i++){
			int cost = search(hist+i*part, query_arr[i], valuemask, max_error, allo[i]);
			if(cost < min_value){
				temp = i;
				min_value = cost;
			}
		}
		greedy[pid] = temp+1;
		allo[temp]++;
	}

	d = palloc(t * sizeof(Datum));
	for (int i = 0; i < t; i++){
		// elog(INFO, " slot %d %d", i, slots[i]);
		d[i] = Int32GetDatum(greedy[i]);
	}
	at = construct_array(d, t, INT4OID, sizeof(int), true, 'i');
	PG_RETURN_POINTER(at);
}
