#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>

#include <nc_core.h>
#include <nc_proto.h>
#include <arpa/inet.h>


#define SSDB_PARAM_1 			1
#define SSDB_PARAM_2 			2
#define SSDB_PARAM_3 			4
#define SSDB_PARAM_4 			8
#define SSDB_PARAM_5 			16
#define SSDB_PARAM_ONE_MORE 	128
#define SSDB_PARAM_TWO_MORE 	256
#define SSDB_PARAM_THREE_MORE 	512
#define SSDB_PARAM_WRITE        1024
#define SSDB_PARAM_MGET         2048
#define SSDB_PARAM_MSET         4096
#define SSDB_PARAM_MDEL         8192
#define SSDB_PARAM_MULTI        16384
#define SSDB_PARAM_STOP_TWO     32768

uint32_t ssdb_command_size = 79;

const char* ssdb_command[] = 
{
    "bitcount",
    "countbit",
    "del",
    "exists",
    "expire",
    "get",
    "getbit",
    "getset",
    "hclear",
    "hdecr",
    "hdel",
    "hexists",
    "hget",
    "hgetall",
    "hincr",
    "hkeys",
    "hlist",
    "hrlist",
    "hrscan",
    "hscan",
    "hset",
    "hsize",
    "incr",
    "multi_del",
    "multi_get",
    "multi_hdel",
    "multi_hget",
    "multi_hset",
    "multi_set",
    "multi_zdel",
    "multi_zget",
    "multi_zset",
    "qback",
    "qclear",
    "qfront",
    "qget",
    "qlist",
    "qpop_back",
    "qpop_front",
    "qpush_back",
    "qpush_front",
    "qrange",
    "qrlist",
    "qset",
    "qsize",
    "qslice",
    "qtrim_back",
    "qtrim_front",
    "set",
    "setbit",
    "setnx",
    "setx",
    "strlen",
    "substr",
    "ttl",
    "zavg",
    "zclear",
    "zcount",
    "zdecr",
    "zdel",
    "zexists",
    "zget",
    "zincr",
    "zkeys",
    "zlist",
    "zpop_back",
    "zpop_front",
    "zrange",
    "zrank",
    "zremrangebyrank",
    "zremrangebyscore",
    "zrlist",
    "zrrange",
    "zrrank",
    "zrscan",
    "zscan",
    "zset",
    "zsize",
    "zsum",
};

int ssdb_command_propery[] =
{
    7,//bitcount
    4,//countbit
    1025,//del
    1,//exists
    2,//expire
    1,//get
    2,//getbit
    1026,//getset
    1025,//hclear
    1030,//hdecr
    1026,//hdel
    2,//hexists
    2,//hget
    1,//hgetall
    1030,//hincr
    8,//hkeys
    4,//hlist
    4,//hrlist
    8,//hrscan
    8,//hscan
    1028,//hset
    1,//hsize
    1027,//incr
    25728,//multi_del
    18560,//multi_get
    1280,//multi_hdel
    256,//multi_hget
    34304,//multi_hset
    54528,//multi_set
    1280,//multi_zdel
    256,//multi_zget
    34304,//multi_zset
    1,//qback
    1025,//qclear
    1,//qfront
    2,//qget
    4,//qlist
    2,//qpop_back
    1026,//qpop_front
    1280,//qpush_back
    1280,//qpush_front
    4,//qrange
    4,//qrlist
    1028,//qset
    1,//qsize
    4,//qslice
    1026,//qtrim_back
    1026,//qtrim_front
    1026,//set
    1028,//setbit
    1026,//setnx
    1028,//setx
    1,//strlen
    4,//substr
    1,//ttl
    4,//zavg
    1025,//zclear
    4,//zcount
    1028,//zdecr
    1026,//zdel
    2,//zexists
    2,//zget
    1028,//zincr
    16,//zkeys
    2,//zlist
    1026,//zpop_back
    1026,//zpop_front
    4,//zrange
    2,//zrank
    1028,//zremrangebyrank
    1028,//zremrangebyscore
    2,//zrlist
    4,//zrrange
    2,//zrrank
    16,//zrscan
    16,//zscan
    1028,//zset
    1,//zsize
    4,//zsum
};

int ssdb_command_valid(struct msg *r)
{
	uint32_t param = array_n(r->keys) + r->narg;
	if (r->ssdb_type & SSDB_PARAM_ONE_MORE)
	{
		if (param >= 1)
		{
			if (r->ssdb_type & SSDB_PARAM_STOP_TWO)
			{
				if (param % 2 == 1)
				{
					return 0;
				}
				return -1;
			}
			return 0;
		}
	}
	else if(r->ssdb_type & SSDB_PARAM_TWO_MORE)
	{
		if (param >= 2)
		{
			if (r->ssdb_type & SSDB_PARAM_STOP_TWO)
			{
				if (param % 2 == 0)
				{
					return 0;
				}
				return -1;
			}
			return 0;
		}
	}
	else if(r->ssdb_type & SSDB_PARAM_THREE_MORE)
	{
		if (param >= 3)
		{
			if (r->ssdb_type & SSDB_PARAM_STOP_TWO)
			{
				if (param % 2 == 1)
				{
					return 0;
				}
				return -1;
			}
			return 0;
		}
	}
	else
	{
		if (param >= 1 && param <=5)
		{
			switch(param)
			{
			case 1:
				if (r->ssdb_type & SSDB_PARAM_1)
				{
					return 0;
				}
				break;
			case 2:
				if (r->ssdb_type & SSDB_PARAM_2)
				{
					return 0;
				}
				break;
			case 3:
				if (r->ssdb_type & SSDB_PARAM_3)
				{
					return 0;
				}
				break;
			case 4:
				if (r->ssdb_type & SSDB_PARAM_4)
				{
					return 0;
				}
				break;
			case 5:
				if (r->ssdb_type & SSDB_PARAM_5)
				{
					return 0;
				}
				break;
			}
		}
	}
	return -1;
}

int get_ssdb_command_property(const char* cmd, uint32_t cmd_len)
{
	int i = 0; 
	int j = ssdb_command_size - 1;
	
	while (i <= j)
	{
		int target = (i + j) / 2;
		const char* command = ssdb_command[target];
		int n = strncmp(command, cmd, cmd_len);	
		
		if (0 == n && command[cmd_len] == 0)
		{
			return ssdb_command_propery[target];
		}
		else if(n < 0)
		{
			i = target + 1;
		}
		else
		{
			j = target - 1;
		}
	}
	
	return -1;
}

void ssdb_parse_req(struct msg *r)
{
	enum {
        SW_START,
		SW_KEY_LENGTH_START,
        SW_KEY_LENGTH,
        SW_KEY,
		SW_ARG_LENGTH_START,
		SW_ARG_LENGTH,
		SW_ARG,
		SW_ARGN_LENGTH_START,
		SW_ARGN_LENGTH,
		SW_ARGN
    } state;
	struct mbuf *b;
	uint8_t *p, *m = NULL;
    uint8_t ch;
	
	state = r->state;
	b = STAILQ_LAST(&r->mhdr, mbuf, next);
	
	for (p = r->pos; p < b->last; p++)
	{
        ch = *p;
		switch(state)
		{
		case SW_START:
			if (isdigit(ch))
			{
				r->ssdb_digit = ch - '0';
				state = SW_KEY_LENGTH;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_START error");
				goto error;
			}
			break;
		case SW_KEY_LENGTH_START:
			if (isdigit(ch))
			{
				r->ssdb_digit = ch - '0';
                state = SW_KEY_LENGTH;	
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_KEY_LENGTH_START error");
				goto error;
			}
			break;
		case SW_KEY_LENGTH:
			if (isdigit(ch))
			{
				r->ssdb_digit = r->ssdb_digit * 10 + ch - '0';
				continue;
			}
			else if (ch == '\n')
			{
				state = SW_KEY;
				r->token = NULL;
				m = p + 1;
				r->ssdb_key_size = 0;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_KEY_LENGTH error");
				goto error;
			}
			break;
		case SW_KEY:
			if (r->token == NULL) {
                r->token = p;
            }
			m = r->token + r->ssdb_digit;
            if (m >= b->last) {
                m = b->last - 1;
                p = m;
                break;
            }
			if (*m != '\n') {
				log_debug(LOG_WARN, "ssdb SW_KEY error");
                goto error;
            }

            p = m; /* move forward by rlen bytes */
            r->rlen = 0;
            m = r->token;
            r->token = NULL;
            r->ssdb_type = -1;
			
			r->ssdb_type  = get_ssdb_command_property((const char*)m, (uint32_t)(p - m));
			if (r->ssdb_type  < 0)
			{
				log_debug(LOG_WARN, "unknown ssdb command %.*s", (uint32_t)(p - m), (const char*)m);
				goto error;
			}
			
			log_debug(LOG_INFO, "ssdb command %.*s", (uint32_t)(p - m), (const char*)m);
			
			r->write = r->ssdb_type & SSDB_PARAM_WRITE ? 1 : 0;
			
			state = SW_ARG_LENGTH_START;
			
			break;
		case SW_ARG_LENGTH_START:
			if (isdigit(ch))
			{
				r->ssdb_digit = ch - '0';
                state = SW_ARG_LENGTH;	
			}
			else if(ch == '\n')
			{
				state = SW_START;
				goto done;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_ARG_LENGTH_START error");
				goto error;
			}
			break;
		case SW_ARG_LENGTH:
			if (isdigit(ch))
			{
				r->ssdb_digit = r->ssdb_digit * 10 + ch - '0';
				continue;
			}
			else if (ch == '\n')
			{
				state = SW_ARG;
				r->ssdb_key_size = 0;
				r->token = NULL;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_ARG_LENGTH error");
				goto error;
			}
			break;
		case SW_ARG:
			if (r->token == NULL)
			{
				r->token = p;
			}
			
			m = r->token + r->ssdb_digit;
			if (m >= b->last) {
                m = b->last - 1;
                p = m;
                break;
            }
			
			if (*m == '\n')
			{
				
				{
					p = m;      
					r->rlen = 0;
					m = r->token;
					r->token = NULL;
					struct keypos *kpos;

					kpos = array_push(r->keys);
					if (kpos == NULL) {
						goto enomem;
					}
					kpos->start = m;
					kpos->end = p;
                }
				
				r->ssdb_digit = 0;
				if (r->ssdb_type & SSDB_PARAM_MULTI)
				{
					state = SW_ARG_LENGTH_START;
				}
				else
				{
				    state = SW_ARGN_LENGTH_START;	
				}
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_ARG_LENGTH error");
				goto error;
			}
			break;
		case SW_ARGN_LENGTH_START:
			if (isdigit(ch))
			{
				r->ssdb_digit = ch - '0';
                state = SW_ARG_LENGTH;	
			}
			else if (ch == '\n')
			{
				state = SW_START;
				goto done;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_ARGN_LENGTH_START error");
				goto error;
			}
			break;
		case SW_ARGN_LENGTH:
			if (isdigit(ch))
			{
				r->ssdb_digit = r->ssdb_digit * 10 + ch - '0';
				continue;
			}
			else if (ch == '\n')
			{
				state = SW_ARG;
				m = p + 1;
				r->ssdb_key_size = 0;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_ARGN_LENGTH error");
				goto error;
			}
			break;
		case SW_ARGN:
			if (ch == '\n')
			{
				if (r->ssdb_key_size  != r->ssdb_digit)
				{
					log_debug(LOG_WARN, "ssdb SW_ARGN error");
					goto error;
				}
				state = SW_ARGN_LENGTH_START;
				r->ssdb_digit = 0;
				r->narg++;
			}
			else
			{
				r->ssdb_key_size++;
				if (r->ssdb_key_size > r->ssdb_digit)
				{
					log_debug(LOG_WARN, "ssdb SW_ARGN error 2");
					goto error;
				}
			}
			break;
		}
	}

	ASSERT(p == b->last);
	r->pos = p;
	r->state = state;

	
	if (b->last == b->end && r->token != NULL) {
		r->pos = r->token;
		r->token = NULL;
		r->result = MSG_PARSE_REPAIR;
	} else {
		r->result = MSG_PARSE_AGAIN;
	}
	return;
done:
	if (0 != ssdb_command_valid(r))
	{
		goto error;
	}
	r->pos = p+1;
	ASSERT(r->pos <= b->last);
	r->state = SW_START;
	r->token = NULL;
	r->result = MSG_PARSE_OK;
	return;
enomem:
    r->result = MSG_PARSE_ERROR;
    r->state = state;


    return;
error:
    r->result = MSG_PARSE_OK;
	r->pos = b->last;
	r->noforward = 1;
    r->state = state;
    errno = EINVAL;
}

void ssdb_parse_rsp(struct msg *r)
{
	enum {
        SW_START,
		SW_KEY_LENGTH_START,
        SW_KEY_LENGTH,
        SW_KEY
    } state;
	struct mbuf *b;
	uint8_t *p, *m = NULL;
    uint8_t ch;
	
	state = r->state;
	b = STAILQ_LAST(&r->mhdr, mbuf, next);
	
	for (p = r->pos; p < b->last; p++)
	{
        ch = *p;
		switch(state)
		{
		case SW_START:
			if (isdigit(ch))
			{
				r->ssdb_digit = ch - '0';
				state = SW_KEY_LENGTH;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_START error");
				goto error;
			}
			break;
		case SW_KEY_LENGTH_START:
			if (isdigit(ch))
			{
				r->ssdb_digit = ch - '0';
                state = SW_KEY_LENGTH;	
			}
			else if(ch == '\n')
			{
				goto done;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_KEY_LENGTH_START error");
				goto error;
			}
			break;
		case SW_KEY_LENGTH:
			if (isdigit(ch))
			{
				r->ssdb_digit = r->ssdb_digit * 10 + ch - '0';
				continue;
			}
			else if (ch == '\n')
			{
				state = SW_KEY;
				m = p + 1;
				r->ssdb_key_size = 0;
			}
			else
			{
				log_debug(LOG_WARN, "ssdb SW_KEY_LENGTH error");
				goto error;
			}
			break;
		case SW_KEY:
			m = p + r->ssdb_digit - r->ssdb_key_size;
            if (m >= b->last) {
                m = b->last - 1;
                r->ssdb_key_size += m - p + 1;
				p = m;
				
                break;
            }
			if (*m != '\n') {
				log_debug(LOG_WARN, "ssdb SW_KEY error");
                goto error;
            }
			p = m;
			state = SW_KEY_LENGTH_START;
			r->ssdb_digit = 0;
			break;
		}
	}
	ASSERT(p == b->last);
	r->pos = p;
	r->state = state;
	
	r->result = MSG_PARSE_AGAIN;
	
	return;
done:
	r->pos = p+1;
	ASSERT(r->pos <= b->last);
	r->state = SW_START;
	r->token = NULL;
	r->result = MSG_PARSE_OK;
	return;
error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;
}

bool ssdb_failure(struct msg *r)
{
	return false;
}


static rstatus_t
ssdb_copy_bulk(struct msg *dst, struct msg *src)
{
    struct mbuf *mbuf, *nbuf;
    uint8_t *p;
    uint32_t len = 0;

    for (mbuf = STAILQ_FIRST(&src->mhdr);
         mbuf && mbuf_empty(mbuf);
         mbuf = STAILQ_FIRST(&src->mhdr)) {

        mbuf_remove(&src->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    mbuf = STAILQ_FIRST(&src->mhdr);
    if (mbuf == NULL) {
        return NC_ERROR;
    }

	len = mbuf_length(mbuf);
	if (len <= 5)
	{
		return NC_ERROR;
	}
	
	p = mbuf->pos + 4;
	
    if (*p != '\n')
	{
		return NC_ERROR;
	}
	
	mbuf->pos += 5;

    /* copy len bytes to dst */
    for (; mbuf;) {
		nbuf = STAILQ_NEXT(mbuf, next);
		mbuf_remove(&src->mhdr, mbuf);
		if (dst != NULL) {
			mbuf_insert(&dst->mhdr, mbuf);
		}
		len = mbuf_length(mbuf);
		dst->mlen += len;
		mbuf = nbuf;
    }

    src->mlen = 0;
	
	mbuf = STAILQ_LAST(&dst->mhdr, mbuf, next);
	
	if (mbuf != NULL)
	{
		mbuf->last--;
	}
    return NC_OK;
}

static rstatus_t ssdb_is_ok(struct msg *src)
{
	struct mbuf *mbuf, *nbuf;
    uint8_t *p;
    uint32_t len = 0;

    for (mbuf = STAILQ_FIRST(&src->mhdr);
         mbuf && mbuf_empty(mbuf);
         mbuf = STAILQ_FIRST(&src->mhdr)) {

        mbuf_remove(&src->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    mbuf = STAILQ_FIRST(&src->mhdr);
    if (mbuf == NULL) {
        return NC_ERROR;
    }

	len = mbuf_length(mbuf);
	if (len <= 5)
	{
		return NC_ERROR;
	}
	
	src->mlen = 0;
	
	p = mbuf->pos;
	if (*p == '2' &&
	    *(p+1) == '\n' &&
		*(p+2) == 'o' &&
		*(p+3) == 'k' &&
		*(p+4) == '\n')
	{
		for (; mbuf;) {
			nbuf = STAILQ_NEXT(mbuf, next);
			mbuf_remove(&src->mhdr, mbuf);
			mbuf_put(mbuf);
			mbuf = nbuf;
		}
		return NC_OK;
	}
	
	return NC_ERROR;
}

void ssdb_pre_coalesce(struct msg *r)
{
	struct msg *pr = r->peer; /* peer request */

    ASSERT(!r->request);
    ASSERT(pr->request);

    if (pr->frag_id == 0) {
        /* do nothing, if not a response to a fragmented request */
        return;
    }
    pr->frag_owner->nfrag_done++;
}

void ssdb_post_coalesce_mget(struct msg *request)
{
	struct msg *response = request->peer;
    struct msg *sub_msg;
    rstatus_t status;
    uint32_t i;

    status = msg_prepend_format(response, "2\nok\n");
    if (status != NC_OK) {
        response->owner->err = 1;
        return;
    }
    
    for (i = 0; i < array_n(request->keys); i++) {      /* for each key */
        sub_msg = request->frag_seq[i]->peer;           /* get it's peer response */
        if (sub_msg == NULL) {
            response->owner->err = 1;
            return;
        }
		if (1 == sub_msg->done)
		{
			continue;
		}
        status = ssdb_copy_bulk(response, sub_msg);
        if (status != NC_OK) {
            response->owner->err = 1;
            return;
        }
		sub_msg->done = 1;
    }
	
	status = msg_append(response, (uint8_t*)"\n", 1);
    if (status != NC_OK) {
        response->owner->err = 1;
        return;
    }
}

void ssdb_post_coalesce_mset(struct msg *request)
{
	struct msg *response = request->peer;
    struct msg *sub_msg;
    rstatus_t status;
	uint32_t frag = 0;
    uint32_t i, len;
	uint8_t tmp[128];
	uint8_t tmp2[128];

    status = msg_prepend_format(response, "2\nok\n");
    if (status != NC_OK) {
        response->owner->err = 1;
        return;
    }

    for (i = 0; i < array_n(request->keys); i+=2) {      /* for each key */
        sub_msg = request->frag_seq[i]->peer;           /* get it's peer response */
        if (sub_msg == NULL) {
            response->owner->err = 1;
            return;
        }
		if (1 == sub_msg->done)
		{
			continue;
		}
        status = ssdb_is_ok(sub_msg);
        if (status != NC_OK) {
            response->owner->err = 1;
            return;
        }
		sub_msg->done = 1;
    }
	frag = array_n(request->keys) / 2;
	len = sprintf((char*)tmp, "%d", frag);
	len = sprintf((char*)tmp2, "%d\n%d\n\n", len, frag);
	status = msg_append(response, tmp2, len);
    if (status != NC_OK) {
        response->owner->err = 1;
        return;
    }
}

void ssdb_post_coalesce(struct msg *request)
{
	if (request->type & SSDB_PARAM_MGET)
	{
		ssdb_post_coalesce_mget(request);
	}
	else if (request->type & SSDB_PARAM_MSET)
	{
		ssdb_post_coalesce_mset(request);
	}
	else if (request->type & SSDB_PARAM_MDEL)
	{
		ssdb_post_coalesce_mset(request);
	}
}

rstatus_t ssdb_add_auth(struct context *ctx, struct conn *c_conn, struct conn *s_conn)
{
	return NC_OK;
}

static rstatus_t
ssdb_append_key(struct msg *r, uint8_t *key, uint32_t keylen)
{
    uint32_t len;
    struct mbuf *mbuf;
    uint8_t printbuf[32];
    struct keypos *kpos;

    /* 1. keylen */
    len = (uint32_t)nc_snprintf(printbuf, sizeof(printbuf), "%d\n", keylen);
    mbuf = msg_ensure_mbuf(r, len);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_copy(mbuf, printbuf, len);
    r->mlen += len;

    /* 2. key */
    mbuf = msg_ensure_mbuf(r, keylen);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    kpos = array_push(r->keys);
    if (kpos == NULL) {
        return NC_ENOMEM;
    }

    kpos->start = mbuf->last;
    kpos->end = mbuf->last + keylen;
    mbuf_copy(mbuf, key, keylen);
    r->mlen += keylen;

    /* 3. CRLF */
    mbuf = msg_ensure_mbuf(r, 1);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }
	char c = '\n';
    mbuf_copy(mbuf, (uint8_t *)&c, 1);
    r->mlen += 1;

    return NC_OK;
}

static rstatus_t
ssdb_fragment_argx(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq,
                    uint32_t key_step)
{
    struct mbuf *mbuf;
    struct msg **sub_msgs;
    uint32_t i;
    rstatus_t status;

    if (array_n(r->keys) % key_step != 0)
	{
		return NC_ERROR;
	}

    sub_msgs = nc_zalloc(ncontinuum * sizeof(*sub_msgs));
    if (sub_msgs == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(r->frag_seq == NULL);
    r->frag_seq = nc_alloc(array_n(r->keys) * sizeof(*r->frag_seq));
    if (r->frag_seq == NULL) {
        nc_free(sub_msgs);
        return NC_ENOMEM;
    }

    mbuf = STAILQ_FIRST(&r->mhdr);
    mbuf->pos = mbuf->start;


    r->frag_id = msg_gen_frag_id();
    r->nfrag = 0;
    r->frag_owner = r;

    for (i = 0; i < array_n(r->keys); i += key_step) {        /* for each key */
        struct msg *sub_msg;
        struct keypos *kpos = array_get(r->keys, i);
        uint32_t idx = msg_backend_idx(r, kpos->start, kpos->end - kpos->start);

        if (sub_msgs[idx] == NULL) {
            sub_msgs[idx] = msg_get(r->owner, r->request, r->protocol);
            if (sub_msgs[idx] == NULL) {
                nc_free(sub_msgs);
                return NC_ENOMEM;
            }
			sub_msgs[idx]->write = r->write;
        }
        r->frag_seq[i] = sub_msg = sub_msgs[idx];

        sub_msg->narg++;
        status = ssdb_append_key(sub_msg, kpos->start, kpos->end - kpos->start);
        if (status != NC_OK) {
            nc_free(sub_msgs);
            return status;
        }

        if (key_step == 1) {                            /* multi_get,del */
            continue;
        } else {                                        /*multi_set*/
			struct keypos *vpos = array_get(r->keys, i+1);
			status = ssdb_append_key(sub_msg, vpos->start, vpos->end - vpos->start);
			if (status != NC_OK) {
				nc_free(sub_msgs);
				return status;
			}
        }
    }

    for (i = 0; i < ncontinuum; i++) {     /* prepend mget header, and forward it */
        struct msg *sub_msg = sub_msgs[i];
        if (sub_msg == NULL) {
            continue;
        }
		if (r->ssdb_type & SSDB_PARAM_MGET)
		{
			status = msg_prepend_format(sub_msg, "9\nmulti_get\n");
		}
		else if (r->ssdb_type & SSDB_PARAM_MSET)
		{
			status = msg_prepend_format(sub_msg, "9\nmulti_set\n");
		}
		else if (r->ssdb_type & SSDB_PARAM_MDEL)
		{
			status = msg_prepend_format(sub_msg, "9\nmulti_del\n");
		}


        if (status != NC_OK) {
            nc_free(sub_msgs);
            return status;
        }
		
		status = msg_append(sub_msg, (uint8_t*)"\n", 1);
		if (status != NC_OK) {
            nc_free(sub_msgs);
            return status;
        }
		
        sub_msg->type = r->ssdb_type;
        sub_msg->frag_id = r->frag_id;
        sub_msg->frag_owner = r->frag_owner;

        TAILQ_INSERT_TAIL(frag_msgq, sub_msg, m_tqe);
        r->nfrag++;
    }

    nc_free(sub_msgs);
    return NC_OK;
}

rstatus_t ssdb_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq)
{
	if (1 == array_n(r->keys)){
        return NC_OK;
    }
	
	if (r->ssdb_type & SSDB_PARAM_MGET)
	{
		return ssdb_fragment_argx(r, ncontinuum, frag_msgq, 1);
	}
	else if (r->ssdb_type & SSDB_PARAM_MSET)
	{
		return ssdb_fragment_argx(r, ncontinuum, frag_msgq, 2);
	}
	else if (r->ssdb_type & SSDB_PARAM_MDEL)
	{
		return ssdb_fragment_argx(r, ncontinuum, frag_msgq, 1);
	}
	else
	{
		return NC_OK;
	}
	
}

rstatus_t ssdb_reply(struct msg *r)
{
	struct msg *response = r->peer;
	return msg_prepend_format(response, "12\nclient_error\n15\nUnknown Command\n\n");
}

void ssdb_post_connect(struct context *ctx, struct conn *conn, struct server *server)
{
	server->connected = 1;
	printf("%.*s connected\n", server->pname.len, server->pname.data);
}

void ssdb_swallow_msg(struct conn *conn, struct msg *pmsg, struct msg *msg)
{
	
}