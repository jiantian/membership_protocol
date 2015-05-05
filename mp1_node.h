/**********************
*
* Progam Name: MP1. Membership Protocol.
* 
* Code authors: Jian Tian
*
* Current file: mp2_node.h
* About this file: Header file.
* 
***********************/

#ifndef _NODE_H_
#define _NODE_H_

#include "stdincludes.h"
#include "params.h"
#include "queue.h"
#include "requests.h"
#include "emulnet.h"

#define HB_INIT 60000   /* default value for heartbeat counter when initialzed */
#define T_GOSSIP 2     /* time step for sending out gossip messages */
#define T_FAIL 25       /* timeout period to detect the node's failure */
#define T_CLEANUP 20    /* period to remove the node from membership list after failure */

/* Configuration Parameters */
char JOINADDR[30];                    /* address for introduction into the group. */
extern char *DEF_SERVADDR;            /* server address. */
extern short PORTNUM;                /* standard portnum of server to contact. */

/* Miscellaneous Parameters */
extern char *STDSTRING;

typedef struct entry_t{
	struct address addr;
	long heartbeat;
	int timestamp;
	int removed;
} entry;

typedef struct member{            
        struct address addr;            // my address
        int inited;                     // boolean indicating if this member is up
        int ingroup;                    // boolean indiciating if this member is in the group

        queue inmsgq;                   // queue for incoming messages

        int bfailed;                    // boolean indicating if this member has failed
		int id;							// node id
		entry** table;					// memberlist table
} member;

/* Message types */
/* Meaning of different message types
  JOINREQ - request to join the group
  JOINREP - replyto JOINREQ
*/
enum Msgtypes{
		JOINREQ,			
		JOINREP,
		GOSSIP,
		DUMMYLASTMSGTYPE
};

/* Generic message template. */
typedef struct messagehdr{ 	
	enum Msgtypes msgtype;
} messagehdr;


/* Functions in mp2_node.c */

/* Message processing routines. */
STDCLLBKRET Process_joinreq STDCLLBKARGS;
STDCLLBKRET Process_joinrep STDCLLBKARGS;
STDCLLBKRET Process_gossip STDCLLBKARGS;

/*
int recv_callback(void *env, char *data, int size);
int init_thisnode(member *thisnode, address *joinaddr);
*/

/*
Other routines.
*/

void nodestart(member *node, char *servaddrstr, short servport);
void nodeloop(member *node);
int recvloop(member *node);
int finishup_thisnode(member *node);
void addEntry(member *node, int id, struct address addr, long heartbeat);
void SerializeMsg(member* node, messagehdr *msg);

#endif /* _NODE_H_ */

