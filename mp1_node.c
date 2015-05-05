/**********************
*
* Progam Name: MP1. Membership Protocol
* 
* Code authors: Jian Tian
*
* Current file: mp1_node.c
* About this file: Member Node Implementation
* 
***********************/

#include "mp1_node.h"
#include "emulnet.h"
#include "MPtemplate.h"
#include "log.h"


/*
 *
 * Routines for introducer and current time.
 *
 */

char NULLADDR[] = {0,0,0,0,0,0};
int isnulladdr( address *addr){
    return (memcmp(addr, NULLADDR, 6)==0?1:0);
}

int ID = 0;

/* 
Return the address of the introducer member. 
*/
address getjoinaddr(void){

    address joinaddr;

    memset(&joinaddr, 0, sizeof(address));
    *(int *)(&joinaddr.addr)=1;
    *(short *)(&joinaddr.addr[4])=0;

    return joinaddr;
}

/*
 *
 * Message Processing routines.
 *
 */

/* serialze the message that will be sent */
void SerializeMsg(member* node, messagehdr *msg) {
	int i;
	size_t sz = 0;
	int removed = 0;
	for (i = 0; i < MAX_NNB; i++) {
		if (node->table[i] != NULL){
			memcpy((char *)(msg+1)+sz, &node->table[i]->addr, sizeof(address));
			memcpy((char *)(msg+1)+sz+sizeof(address), &node->table[i]->heartbeat, sizeof(long));
			memcpy((char *)(msg+1)+sz+sizeof(address)+sizeof(long), &node->table[i]->timestamp, sizeof(int));
			memcpy((char *)(msg+1)+sz+sizeof(address)+sizeof(long)+sizeof(int), &i, sizeof(int));
			memcpy((char *)(msg+1)+sz+sizeof(address)+sizeof(long)+sizeof(int)+sizeof(int), &removed, sizeof(int));
			sz+=sizeof(entry)+sizeof(int);
		}
	}
}

/* 
Received a JOINREQ (joinrequest) message.
*/
void Process_joinreq(void *env, char *data, int size)
{
	member *node = (member *) env;
    messagehdr *msg;
    address addedAddr;
    int id;

	memcpy(&addedAddr, data, sizeof(address));
	memcpy(&id, data+sizeof(address), sizeof(int));

	/* send its membership list to the requesting node */
	size_t msgsize = sizeof(messagehdr);
	size_t sz = 0;
	int i;
    for (i = 0; i < MAX_NNB; i++) {
        if (node->table[i] != NULL){
            sz+=sizeof(entry)+sizeof(int);
        }
    }
	msgsize+=sz;
	msg = malloc(msgsize);
	msg->msgtype=JOINREP;
	SerializeMsg(node, msg);
	MPp2psend(&node->addr, &addedAddr, (char *)msg, msgsize);

	/* add the new member to its list */
	addEntry(node, id, addedAddr, HB_INIT);	
#ifdef DEBUGLOG
    logNodeAdd(&node->addr, &addedAddr);
#endif

	free(msg);
    return;
}

/* 
Received a JOINREP (joinreply) message. 
*/
void Process_joinrep(void *env, char *data, int size)
{
	member *node = (member *) env;
	size_t sz;
	int id, timestamp,removed;
	long heartbeat;
	address addr;

	/* parse the data and add the member to its membership list */
	for (sz = 0; sz < size; sz+=(sizeof(entry)+sizeof(int))) {
		memcpy(&addr, data+sz, sizeof(address));
		memcpy(&heartbeat, data+sz+sizeof(address), sizeof(long));
		memcpy(&timestamp, data+sz+sizeof(address)+sizeof(long), sizeof(int));
		memcpy(&id, data+sz+sizeof(address)+sizeof(long)+sizeof(int), sizeof(int));
		memcpy(&removed, data+sz+sizeof(address)+sizeof(long)+sizeof(int)+sizeof(int), sizeof(int));
		addEntry(node, id, addr, heartbeat);
#ifdef DEBUGLOG
    		logNodeAdd(&node->addr, &addr);
#endif
	}

	node->ingroup = 1;
    return;
}

/*
Received a GOSSIP (gossip) message. 
*/
void Process_gossip(void *env, char *data, int size)
{
	member *node = (member *) env;
	int id, timestamp, removed;
	long heartbeat;
	address addr;
	size_t sz;

	/* parse the data and perform add/update to the membership list */
	for (sz = 0; sz < size; sz+=(sizeof(entry)+sizeof(int))) {
		memcpy(&addr, data+sz, sizeof(address));
        memcpy(&heartbeat, data+sz+sizeof(address), sizeof(long));
        memcpy(&timestamp, data+sz+sizeof(address)+sizeof(long), sizeof(int));
        memcpy(&id, data+sz+sizeof(address)+sizeof(long)+sizeof(int), sizeof(int));
		memcpy(&removed, data+sz+sizeof(address)+sizeof(long)+sizeof(int)+sizeof(int), sizeof(int));
		if (node->table[id] == NULL) { /* no member entry yet; add the member */
			addEntry(node, id, addr, heartbeat);
   		    #ifdef DEBUGLOG
            	logNodeAdd(&node->addr, &addr);
        	#endif
		}
		else { /* we need to update the membership list */
			if (heartbeat > node->table[id]->heartbeat && (getcurrtime() - node->table[id]->timestamp) <= T_FAIL ){ /* only update when receiving higher heartbeat counter */
				node->table[id]->heartbeat = heartbeat;
				node->table[id]->timestamp = getcurrtime();
			}
		}
	}

    return;
}

/* 
Array of Message handlers. 
*/
void ( ( * MsgHandler [20] ) STDCLLBKARGS )={
/* Message processing operations at the P2P layer. */
    Process_joinreq, 
    Process_joinrep,
	Process_gossip
};

/* 
Called from nodeloop() on each received packet dequeue()-ed from node->inmsgq. 
Parse the packet, extract information and process. 
env is member *node, data is 'messagehdr'. 
*/
int recv_callback(void *env, char *data, int size){

    member *node = (member *) env;
    messagehdr *msghdr = (messagehdr *)data;
    char *pktdata = (char *)(msghdr+1);

    if(size < sizeof(messagehdr)){
#ifdef DEBUGLOG
        LOG(&((member *)env)->addr, "Faulty packet received - ignoring");
#endif
        return -1;
    }

#ifdef DEBUGLOG
    LOG(&((member *)env)->addr, "Received msg type %d with %d B payload", msghdr->msgtype, size - sizeof(messagehdr));
#endif

    if((node->ingroup && msghdr->msgtype >= 0 && msghdr->msgtype <= DUMMYLASTMSGTYPE)
        || (!node->ingroup && msghdr->msgtype==JOINREP))            
            /* if not yet in group, accept only JOINREPs */
        MsgHandler[msghdr->msgtype](env, pktdata, size-sizeof(messagehdr));
    /* else ignore (garbled message) */
    free(data);

    return 0;

}

/*
 *
 * Initialization and cleanup routines.
 *
 */

/* 
Find out who I am, and start up. 
*/
int init_thisnode(member *thisnode, address *joinaddr){
    
    if(MPinit(&thisnode->addr, PORTNUM, (char *)joinaddr)== NULL){ /* Calls ENInit */
#ifdef DEBUGLOG
        LOG(&thisnode->addr, "MPInit failed");
#endif
        exit(1);
    }
#ifdef DEBUGLOG
    else LOG(&thisnode->addr, "MPInit succeeded. Hello.");
#endif

    thisnode->bfailed=0;
    thisnode->inited=1;
    thisnode->ingroup=0;
	thisnode->id = ID++;
    /* node is up! */

	/* initialize membership list table */
	thisnode->table = malloc(sizeof(entry)*MAX_NNB);
	int i;
	for (i = 0; i < MAX_NNB; i++) {
		thisnode->table[i] = NULL;
	}

	/* add itself to membership list table */
	addEntry(thisnode, thisnode->id, thisnode->addr, HB_INIT);
#ifdef DEBUGLOG
    logNodeAdd(&thisnode->addr, &thisnode->addr);
#endif

    return 0;
}

/*
Helper function to add an entry in membership list table
*/
void addEntry(member *node, int id, struct address addr, long heartbeat) {
	node->table[id] = malloc(sizeof(entry));
	memcpy(&node->table[id]->addr, &addr, sizeof(address));
	node->table[id]->heartbeat = heartbeat;
	node->table[id]->timestamp = getcurrtime();
	node->table[id]->removed = 0;
}

/* 
Clean up this node. 
*/
int finishup_thisnode(member *node){

	int i;
	for (i = 0; i < MAX_NNB; i++) {
		if (node->table[i] != NULL) {
			free(node->table[i]);
			node->table[i] = NULL;
		}
	}
	free(node->table);

    return 0;
}


/* 
 *
 * Main code for a node 
 *
 */

/* 
Introduce self to group. 
*/
int introduceselftogroup(member *node, address *joinaddr){
    
    messagehdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if(memcmp(&node->addr, joinaddr, 4*sizeof(char)) == 0){
        /* I am the group booter (first process to join the group). Boot up the group. */
#ifdef DEBUGLOG
        LOG(&node->addr, "Starting up group...");
#endif

        node->ingroup = 1;
    }
    else{
        size_t msgsize = sizeof(messagehdr) + sizeof(address) + sizeof(int);
        msg=malloc(msgsize);

    /* create JOINREQ message: format of data is {struct address myaddr} */
        msg->msgtype=JOINREQ;
        memcpy((char *)(msg+1), &node->addr, sizeof(address));
		memcpy((char *)(msg+1)+sizeof(address), &node->id, sizeof(int));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        LOG(&node->addr, s);
#endif

    /* send JOINREQ message to introducer member. */
        MPp2psend(&node->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }

    return 1;

}

/* 
Called from nodeloop(). 
*/
void checkmsgs(member *node){
    void *data;
    int size;

    /* Dequeue waiting messages from node->inmsgq and process them. */
	
    while((data = dequeue(&node->inmsgq, &size)) != NULL) {
        recv_callback((void *)node, data, size); 
    }
    return;
}


/* 
Executed periodically for each member. 
Performs necessary periodic operations. 
Called by nodeloop(). 
*/
void nodeloopops(member *node){

	int i, j, gossip1, gossip2, gossip3;
	size_t msgsize;
	size_t sz;
	messagehdr *msg;

	/* check the memberlist table to see if any node times out and delete it*/
	for (i = 0; i < MAX_NNB; i++) {
		if (node->table[i] != NULL) {
			if ((getcurrtime()-node->table[i]->timestamp > (T_FAIL+T_CLEANUP)) && !node->table[i]->removed) {
#ifdef DEBUGLOG
                logNodeRemove(&node->addr, &node->table[i]->addr);
#endif
				node->table[i]->removed = 1;
			}
		}
	}	

	/* at each T_GOSSIP time, send out gossip messages to 3 random nodes */
	if (getcurrtime() % T_GOSSIP == 0) {

		/* select random nodes */
		gossip1 = rand() % MAX_NNB;
		do {
			gossip2 = rand() % MAX_NNB;
		}
		while (gossip2 == gossip1);
		do {
			gossip3 = rand() % MAX_NNB;
		}
		while (gossip3 == gossip2 || gossip3 == gossip1);
    	int array[3] = {gossip1, gossip2, gossip3}; /* get the ids of the nodes that the current node will send gossip message to */

		/* update the heartbeat counter for this node */
		node->table[node->id]->heartbeat++;
		node->table[node->id]->timestamp = getcurrtime();
		/* prepare message */
		msgsize = sizeof(messagehdr);
        sz = 0;
        for (j = 0; j < MAX_NNB; j++) {
            if (node->table[j] != NULL){
                sz+=sizeof(entry)+sizeof(int);
            }
        }
        msgsize+=sz;
        msg = malloc(msgsize);
        msg->msgtype=GOSSIP;
        SerializeMsg(node, msg);
		/* send its membership list to the random node */
		for (i = 0; i < 3; i++) {
			if (node->table[array[i]]!=NULL) {
		    	MPp2psend(&node->addr, &node->table[array[i]]->addr, (char *)msg, msgsize);
			}
		}
		free(msg);
	}

    return;
}

/* 
Executed periodically at each member. Called from app.c.
*/
void nodeloop(member *node){
    if (node->bfailed) return;

    checkmsgs(node);

    /* Wait until you're in the group... */
    if(!node->ingroup) return ;

    /* ...then jump in and share your responsibilites! */
    nodeloopops(node);
    
    return;
}

/* 
All initialization routines for a member. Called by app.c. 
*/
void nodestart(member *node, char *servaddrstr, short servport){

    address joinaddr=getjoinaddr();

    /* Self booting routines */
    if(init_thisnode(node, &joinaddr) == -1){

#ifdef DEBUGLOG
        LOG(&node->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if(!introduceselftogroup(node, &joinaddr)){
        finishup_thisnode(node);
#ifdef DEBUGLOG
        LOG(&node->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/* 
Enqueue a message (buff) onto the queue env. 
*/
int enqueue_wrppr(void *env, char *buff, int size){    return enqueue((queue *)env, buff, size);}

/* 
Called by a member to receive messages currently waiting for it. 
*/
int recvloop(member *node){
    if (node->bfailed) return -1;
    else return MPrecv(&(node->addr), enqueue_wrppr, NULL, 1, &node->inmsgq); 
    /* Fourth parameter specifies number of times to 'loop'. */
}

