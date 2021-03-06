#include "skynet.h"

#include "socket_server.h"
#include "socket_poll.h"
#include "atomic.h"
#include "spinlock.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#define MAX_INFO 128
// MAX_SOCKET will be 2^MAX_SOCKET_P
#define MAX_SOCKET_P 16
#define MAX_EVENT 64
#define MIN_READ_BUFFER 64
#define SOCKET_TYPE_INVALID 0
#define SOCKET_TYPE_RESERVE 1  // 新创建的套接字，还没初始化时候套接字类型，即用来占用slot位置
#define SOCKET_TYPE_PLISTEN 2 // 创建和初始化套接字后的状态，等待开启监听
#define SOCKET_TYPE_LISTEN 3
#define SOCKET_TYPE_CONNECTING 4
#define SOCKET_TYPE_CONNECTED 5
#define SOCKET_TYPE_HALFCLOSE 6
#define SOCKET_TYPE_PACCEPT 7
#define SOCKET_TYPE_BIND 8

#define MAX_SOCKET (1<<MAX_SOCKET_P) // 当前是2^16，即65536

#define PRIORITY_HIGH 0
#define PRIORITY_LOW 1

#define HASH_ID(id) (((unsigned)id) % MAX_SOCKET)
#define ID_TAG16(id) ((id>>MAX_SOCKET_P) & 0xffff) // 取id中第三和第四个字节的数据

#define PROTOCOL_TCP 0
#define PROTOCOL_UDP 1
#define PROTOCOL_UDPv6 2
#define PROTOCOL_UNKNOWN 255

#define UDP_ADDRESS_SIZE 19	// ipv6 128bit + port 16bit + 1 byte type

#define MAX_UDP_PACKAGE 65535

// EAGAIN and EWOULDBLOCK may be not the same value.
#if (EAGAIN != EWOULDBLOCK)
#define AGAIN_WOULDBLOCK EAGAIN : case EWOULDBLOCK
#else
#define AGAIN_WOULDBLOCK EAGAIN
#endif

#define WARNING_SIZE (1024*1024) // 等待发送的数据初次报警的数值，后面都是上一次数值*2

struct write_buffer {
	struct write_buffer * next;
	void *buffer; // 指向开始构造的dw_buffer
	char *ptr; // 指向实质要发送的数据开始部分，即指向buffer+dw_offset部分
	int sz; // 实质要发送的数据大小
	// userobject为ture ,表示send_object结构体中free_func字段是应用程接管的，
	// 而是通用的skynet_free函数
	bool userobject;
	uint8_t udp_address[UDP_ADDRESS_SIZE];
};

// 宏offsetof返回成员udp_address在结构体write_buffer的偏移量
#define SIZEOF_TCPBUFFER (offsetof(struct write_buffer, udp_address[0]))
#define SIZEOF_UDPBUFFER (sizeof(struct write_buffer))

struct wb_list {
	struct write_buffer * head;
	struct write_buffer * tail;
};

struct socket_stat {
	uint64_t rtime;
	uint64_t wtime; // 最近往网络写数据的时间
	uint64_t read;
	uint64_t write; // 统计往网络写入的字节总数
};

struct socket {
	uintptr_t opaque;     // 与本socket关联的服务handle
	struct wb_list high; // 用于保存高优先级数据的 write_buffer list
	struct wb_list low; // 用于保存低优先级数据的 write_buffer list
	int64_t wb_size; // 保存所有要发送的数据字节数，包括high和low字段所有的数据
	struct socket_stat stat;
	volatile uint32_t sending; // 这个字段的第三个字节和第四个字节值与id一样，低的两个字节初始值为0
	int fd;
	int id; // 通过HASH_ID(id)，可以获得在数组slot中的下标
	uint8_t protocol; // 使用的协议，值为PROTOCOL_TCP等类型
	uint8_t type; // socket 当前状态类型，初始值为SOCKET_TYPE_INVALID，epoll事件触发时，会根据type来选择处理事件的逻辑
	uint16_t udpconnecting;
	int64_t warn_size; // 累计等待要发送的数据量，报警的数值
	union {
		int size; // 保存下次从网络上读数据最大的大小
		uint8_t udp_address[UDP_ADDRESS_SIZE];
	} p;
	struct spinlock dw_lock;
	int dw_offset; // 保存在工作线程中已经写完成的数据
 	// 用于保存工作线程没发送完剩余的数据，
	// 指向的内容，除了要发送的数据外，最前面还包括其他信息，长度为dw_offset
	const void * dw_buffer;
	size_t dw_size; // dw_buffer 总的大小
};

struct socket_server {
	volatile uint64_t time; // 保存skynet启动以来，经过的厘秒数
	int recvctrl_fd; // 用于接收命令行数据的 fd，即管道的读端
	int sendctrl_fd; // 用于发送命令行数据的 fd，即管道的写端
	int checkctrl; // 用于表示是否检查处理命令行相关数据，初始值为1
	poll_fd event_fd; // epoll 对应的 fd
	int alloc_id; // 初始值为0，一直递增的，用来给新的套接字在slot数组中找一个空的位置
	int event_n; // 初始值为0,标记本次epoll事件的数量
	int event_index; // 初始化为0，下一个未处理的epoll事件索引
	struct socket_object_interface soi; // 用来接管send_object的生成，即接口send_object_init中使用
	struct event ev[MAX_EVENT]; // 保存当前可读写的事件信息，即保存epoll_wait的结果
	struct socket slot[MAX_SOCKET]; // 保存所有套接字
	// 用来暂时保存一些数据，比如在connect和accept的时候，保存对方的ip地址和端口信息
	char buffer[MAX_INFO]; 
	uint8_t udpbuffer[MAX_UDP_PACKAGE];
	fd_set rfds;
};

// { worker线程向socket线程发送请求，请求数据的使用结构体，不用的请求用不同的结构体封装
struct request_open {
	int id;
	int port;
	uintptr_t opaque;
	char host[1];
};

struct request_send {
	int id;
	int sz;
	char * buffer;
};

struct request_send_udp {
	struct request_send send;
	uint8_t address[UDP_ADDRESS_SIZE];
};

struct request_setudp {
	int id;
	uint8_t address[UDP_ADDRESS_SIZE];
};

struct request_close {
	int id;
	int shutdown;
	uintptr_t opaque;
};

struct request_listen {
	int id;
	int fd;
	uintptr_t opaque; // 通常是服务的handle
	char host[1];
};

struct request_bind {
	int id;
	int fd;
	uintptr_t opaque;
};

struct request_start {
	int id;
	uintptr_t opaque;
};

struct request_setopt {
	int id;
	int what;
	int value;
};

struct request_udp {
	int id;
	int fd;
	int family;
	uintptr_t opaque;
};

/*
	The first byte is TYPE

	S Start socket
	B Bind socket
	L Listen socket
	K Close socket
	O Connect to (Open)
	X Exit
	D Send package (high)
	P Send package (low)
	A Send UDP package
	T Set opt
	U Create UDP socket
	C set udp address
	Q query info
 */

struct request_package {
	uint8_t header[8];	// 6 bytes dummy
	union {
		char buffer[256];
		struct request_open open; // 'O'
		struct request_send send;
		struct request_send_udp send_udp;
		struct request_close close;
		struct request_listen listen;
		struct request_bind bind;
		struct request_start start;
		struct request_setopt setopt;
		struct request_udp udp;
		struct request_setudp set_udp;
	} u;
	uint8_t dummy[256];
};

// worker线程向socket线程发送请求结构体}

union sockaddr_all {
	struct sockaddr s;
	struct sockaddr_in v4;
	struct sockaddr_in6 v6;
};

// 作为发送数据的中间形式，即要发送的数据 转换成这个结构体(send_object_init来转换)
// 然后发送的数据逻辑，处理这个结构体即可
struct send_object {
	void * buffer;
	int sz;
	void (*free_func)(void *);
};

#define MALLOC skynet_malloc
#define FREE skynet_free

struct socket_lock {
	struct spinlock *lock; // 指向socket对应的锁，即结构体socket字段dw_lock
	int count;
};

static inline void
socket_lock_init(struct socket *s, struct socket_lock *sl) {
	sl->lock = &s->dw_lock;
	sl->count = 0;
}

static inline void
socket_lock(struct socket_lock *sl) {
	if (sl->count == 0) {
		spinlock_lock(sl->lock);
	}
	++sl->count;
}

static inline int
socket_trylock(struct socket_lock *sl) {
	if (sl->count == 0) {
		if (!spinlock_trylock(sl->lock))
			return 0;	// lock failed
	}
	++sl->count;
	return 1;
}

static inline void
socket_unlock(struct socket_lock *sl) {
	--sl->count;
	if (sl->count <= 0) {
		assert(sl->count == 0);
		spinlock_unlock(sl->lock);
	}
}

// 把要发送的数据信息，都统一转换到结构体send_object中
static inline bool
send_object_init(struct socket_server *ss, struct send_object *so, void *object, int sz) {
	if (sz < 0) {
		so->buffer = ss->soi.buffer(object);
		so->sz = ss->soi.size(object);
		so->free_func = ss->soi.free;
		return true;
	} else {
		so->buffer = object;
		so->sz = sz;
		so->free_func = FREE;
		return false;
	}
}

static inline void
write_buffer_free(struct socket_server *ss, struct write_buffer *wb) {
	if (wb->userobject) {
		ss->soi.free(wb->buffer);
	} else {
		FREE(wb->buffer);
	}
	FREE(wb);
}

static void
socket_keepalive(int fd) {
	int keepalive = 1;
	setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&keepalive , sizeof(keepalive));  
}

// 从管理所有的套接字结构体socket的slot找一个空的位置，并且返回相应的id
// 通过id，可以获得空结构体在slot中的位置
static int
reserve_id(struct socket_server *ss) {
	int i;
	for (i=0;i<MAX_SOCKET;i++) {
		int id = ATOM_INC(&(ss->alloc_id));
		if (id < 0) {
			id = ATOM_AND(&(ss->alloc_id), 0x7fffffff);
		}
		struct socket *s = &ss->slot[HASH_ID(id)];
		if (s->type == SOCKET_TYPE_INVALID) {
			// 找一个空位置
			if (ATOM_CAS(&s->type, SOCKET_TYPE_INVALID, SOCKET_TYPE_RESERVE)) {
				s->id = id;
				s->protocol = PROTOCOL_UNKNOWN;
				// socket_server_udp_connect may inc s->udpconncting directly (from other thread, before new_fd), 
				// so reset it to 0 here rather than in new_fd.
				s->udpconnecting = 0;
				s->fd = -1;
				return id;
			} else {
				// retry
				--i;
			}
		}
	}
	return -1;
}

static inline void
clear_wb_list(struct wb_list *list) {
	list->head = NULL;
	list->tail = NULL;
}

// 在主线程中调用，服务器启动时候调用，创建管理 socket 相关的结构体 socket_server
struct socket_server * 
socket_server_create(uint64_t time) {
	int i;
	int fd[2];
	// 调用系统接口 epoll_create，创建一个 epoll 实例，接口返回一个文件描述符，
	// 返回的 fd 用于后续系统接口调用
	poll_fd efd = sp_create();
	if (sp_invalid(efd)) {
		fprintf(stderr, "socket-server: create event pool failed.\n");
		return NULL;
	}

	// pipe创建一个管道，用于线程间通信，返回两个fd，pipefd[0]用于读，pipefd[1]用于写
	// 写入到管道数据，在读取之前，都是被 Linux 内核 buffed 的
	// 这里是用于worker线程和socket线程通信
	if (pipe(fd)) {
		// sp_release 只是对 close 简单封装
		sp_release(efd);
		fprintf(stderr, "socket-server: create socket pair failed.\n");
		return NULL;
	}

	// 监听管道 读端 是否有数据可读
	if (sp_add(efd, fd[0], NULL)) {
		// add recvctrl_fd to event poll
		fprintf(stderr, "socket-server: can't add server fd to event pool.\n");
		close(fd[0]);
		close(fd[1]);
		sp_release(efd);
		return NULL;
	}

	struct socket_server *ss = MALLOC(sizeof(*ss));
	ss->time = time;
	ss->event_fd = efd;
	ss->recvctrl_fd = fd[0];
	ss->sendctrl_fd = fd[1];
	ss->checkctrl = 1;

	// 处理化字段slot，用来管理各个套接字的数组
	for (i=0;i<MAX_SOCKET;i++) {
		struct socket *s = &ss->slot[i];
		s->type = SOCKET_TYPE_INVALID;
		clear_wb_list(&s->high);
		clear_wb_list(&s->low);
		spinlock_init(&s->dw_lock);
	}
	ss->alloc_id = 0;
	ss->event_n = 0;
	ss->event_index = 0;
	memset(&ss->soi, 0, sizeof(ss->soi));
	FD_ZERO(&ss->rfds);
	assert(ss->recvctrl_fd < FD_SETSIZE);

	return ss;
}

// 在timer线程中轮询调用到
// 更新socket线程保存的skynet系统的时间
void
socket_server_updatetime(struct socket_server *ss, uint64_t time) {
	ss->time = time;
}

// 释放buff list对应的内存，包括对应数据分配的内存
static void
free_wb_list(struct socket_server *ss, struct wb_list *list) {
	struct write_buffer *wb = list->head;
	while (wb) {
		struct write_buffer *tmp = wb;
		wb = wb->next;
		write_buffer_free(ss, tmp);
	}
	list->head = NULL;
	list->tail = NULL;
}

static void
free_buffer(struct socket_server *ss, const void * buffer, int sz) {
	struct send_object so;
	send_object_init(ss, &so, (void *)buffer, sz);
	so.free_func((void *)buffer);
}

// @socket线程，清空套接字结构体对应的相关信息，并且把套接字从epoll中删除监听
// 设置套接字类型为 SOCKET_TYPE_INVALID
static void
force_close(struct socket_server *ss, struct socket *s, struct socket_lock *l, struct socket_message *result) {
	result->id = s->id;
	result->ud = 0;
	result->data = NULL;
	result->opaque = s->opaque;
	if (s->type == SOCKET_TYPE_INVALID) {
		return;
	}
	assert(s->type != SOCKET_TYPE_RESERVE);
	free_wb_list(ss,&s->high);
	free_wb_list(ss,&s->low);
	if (s->type != SOCKET_TYPE_PACCEPT && s->type != SOCKET_TYPE_PLISTEN) {
		sp_del(ss->event_fd, s->fd);
	}
	socket_lock(l);
	if (s->type != SOCKET_TYPE_BIND) {
		if (close(s->fd) < 0) {
			perror("close socket:");
		}
	}
	s->type = SOCKET_TYPE_INVALID;
	if (s->dw_buffer) {
		free_buffer(ss, s->dw_buffer, s->dw_size);
		s->dw_buffer = NULL;
	}
	socket_unlock(l);
}

// 服务器退出的时候，主线程中调用
// 关闭套接字和释放相关资源
void 
socket_server_release(struct socket_server *ss) {
	int i;
	struct socket_message dummy;
	for (i=0;i<MAX_SOCKET;i++) {
		struct socket *s = &ss->slot[i];
		struct socket_lock l;
		socket_lock_init(s, &l);
		if (s->type != SOCKET_TYPE_RESERVE) {
			force_close(ss, s, &l, &dummy);
		}
		spinlock_destroy(&s->dw_lock);
	}
	close(ss->sendctrl_fd);
	close(ss->recvctrl_fd);
	sp_release(ss->event_fd);
	FREE(ss);
}

static inline void
check_wb_list(struct wb_list *s) {
	assert(s->head == NULL);
	assert(s->tail == NULL);
}

// @socket线程：初始化新的套接字结构体sokcet信息
// 根据参数add的值，确定是否把套接字加入到epoll监听读事件中
static struct socket *
new_fd(struct socket_server *ss, int id, int fd, int protocol, uintptr_t opaque, bool add) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	assert(s->type == SOCKET_TYPE_RESERVE);

	if (add) {
		if (sp_add(ss->event_fd, fd, s)) {
			s->type = SOCKET_TYPE_INVALID;
			return NULL;
		}
	}

	s->id = id;
	s->fd = fd;
	s->sending = ID_TAG16(id) << 16 | 0;
	s->protocol = protocol;
	s->p.size = MIN_READ_BUFFER;
	s->opaque = opaque;
	s->wb_size = 0;
	s->warn_size = 0;
	check_wb_list(&s->high);
	check_wb_list(&s->low);
	s->dw_buffer = NULL;
	s->dw_size = 0;
	memset(&s->stat, 0, sizeof(s->stat));
	return s;
}

static inline void
stat_read(struct socket_server *ss, struct socket *s, int n) {
	s->stat.read += n;
	s->stat.rtime = ss->time;
}

static inline void
stat_write(struct socket_server *ss, struct socket *s, int n) {
	s->stat.write += n;
	s->stat.wtime = ss->time;
}

// return -1 when connecting
// @socket线程，处理来自worker线程的请求 'O'
// 创建新的套接字，并且请求connect指定的host和port
static int
open_socket(struct socket_server *ss, struct request_open * request, struct socket_message *result) {
	int id = request->id;
	result->opaque = request->opaque;
	result->id = id;
	result->ud = 0;
	result->data = NULL;
	struct socket *ns;
	int status;
	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	struct addrinfo *ai_ptr = NULL;
	char port[16];
	sprintf(port, "%d", request->port);
	memset(&ai_hints, 0, sizeof( ai_hints ) );
	ai_hints.ai_family = AF_UNSPEC;
	ai_hints.ai_socktype = SOCK_STREAM;
	ai_hints.ai_protocol = IPPROTO_TCP;

	status = getaddrinfo( request->host, port, &ai_hints, &ai_list );
	if ( status != 0 ) {
		result->data = (void *)gai_strerror(status);
		goto _failed;
	}
	int sock= -1;
	for (ai_ptr = ai_list; ai_ptr != NULL; ai_ptr = ai_ptr->ai_next ) {
		sock = socket( ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol );
		if ( sock < 0 ) {
			continue;
		}
		socket_keepalive(sock);
		sp_nonblocking(sock);
		status = connect( sock, ai_ptr->ai_addr, ai_ptr->ai_addrlen);
		if ( status != 0 && errno != EINPROGRESS) {
			close(sock);
			sock = -1;
			continue;
		}

		break;
	}

	if (sock < 0) {
		result->data = strerror(errno);
		goto _failed;
	}

	// 同时监听数据写入
	ns = new_fd(ss, id, sock, PROTOCOL_TCP, request->opaque, true);
	if (ns == NULL) {
		close(sock);
		result->data = "reach skynet socket number limit";
		goto _failed;
	}

	if(status == 0) {
		// 请求连接成功了
		ns->type = SOCKET_TYPE_CONNECTED;
		struct sockaddr * addr = ai_ptr->ai_addr;
		void * sin_addr = (ai_ptr->ai_family == AF_INET) ? (void*)&((struct sockaddr_in *)addr)->sin_addr : (void*)&((struct sockaddr_in6 *)addr)->sin6_addr;
		// inet_ntop 把IPV4 和 IPV6地址从二进制数据转换为文本的形式
		if (inet_ntop(ai_ptr->ai_family, sin_addr, ss->buffer, sizeof(ss->buffer))) {
			result->data = ss->buffer;
		}
		freeaddrinfo( ai_list );
		return SOCKET_OPEN;
	} else {
		// 还没有连接成功，正在请求，监听套接字的写事件
		ns->type = SOCKET_TYPE_CONNECTING;
		sp_write(ss->event_fd, ns->fd, ns, true);
	}

	freeaddrinfo( ai_list );
	return -1;
_failed:
	freeaddrinfo( ai_list );
	ss->slot[HASH_ID(id)].type = SOCKET_TYPE_INVALID;
	return SOCKET_ERR;
}

// @socket线程，往网络写数据，把list中所有的数据发送成功为止，除非网络套接字暂时不可写了
static int
send_list_tcp(struct socket_server *ss, struct socket *s, struct wb_list *list, struct socket_lock *l, struct socket_message *result) {
	while (list->head) {
		struct write_buffer * tmp = list->head;
		for (;;) {
			ssize_t sz = write(s->fd, tmp->ptr, tmp->sz);
			if (sz < 0) {
				switch(errno) {
				case EINTR:
					continue;
				case AGAIN_WOULDBLOCK:
					return -1;
				}
				force_close(ss,s,l,result);
				return SOCKET_CLOSE;
			}
			stat_write(ss,s,(int)sz);
			s->wb_size -= sz;
			if (sz != tmp->sz) {
				// 只发送部分出去，等待下一次发送吧
				tmp->ptr += sz;
				tmp->sz -= sz;
				return -1;
			}
			break;
		}
		// 继续发送下一个节点和释放前一个发送buff数据内存和节点内存
		list->head = tmp->next;
		write_buffer_free(ss,tmp);
	}
	list->tail = NULL;

	return -1;
}

static socklen_t
udp_socket_address(struct socket *s, const uint8_t udp_address[UDP_ADDRESS_SIZE], union sockaddr_all *sa) {
	int type = (uint8_t)udp_address[0];
	if (type != s->protocol)
		return 0;
	uint16_t port = 0;
	memcpy(&port, udp_address+1, sizeof(uint16_t));
	switch (s->protocol) {
	case PROTOCOL_UDP:
		memset(&sa->v4, 0, sizeof(sa->v4));
		sa->s.sa_family = AF_INET;
		sa->v4.sin_port = port;
		memcpy(&sa->v4.sin_addr, udp_address + 1 + sizeof(uint16_t), sizeof(sa->v4.sin_addr));	// ipv4 address is 32 bits
		return sizeof(sa->v4);
	case PROTOCOL_UDPv6:
		memset(&sa->v6, 0, sizeof(sa->v6));
		sa->s.sa_family = AF_INET6;
		sa->v6.sin6_port = port;
		memcpy(&sa->v6.sin6_addr, udp_address + 1 + sizeof(uint16_t), sizeof(sa->v6.sin6_addr)); // ipv6 address is 128 bits
		return sizeof(sa->v6);
	}
	return 0;
}

static void
drop_udp(struct socket_server *ss, struct socket *s, struct wb_list *list, struct write_buffer *tmp) {
	s->wb_size -= tmp->sz;
	list->head = tmp->next;
	if (list->head == NULL)
		list->tail = NULL;
	write_buffer_free(ss,tmp);
}

static int
send_list_udp(struct socket_server *ss, struct socket *s, struct wb_list *list, struct socket_message *result) {
	while (list->head) {
		struct write_buffer * tmp = list->head;
		union sockaddr_all sa;
		socklen_t sasz = udp_socket_address(s, tmp->udp_address, &sa);
		if (sasz == 0) {
			fprintf(stderr, "socket-server : udp (%d) type mismatch.\n", s->id);
			drop_udp(ss, s, list, tmp);
			return -1;
		}
		int err = sendto(s->fd, tmp->ptr, tmp->sz, 0, &sa.s, sasz);
		if (err < 0) {
			switch(errno) {
			case EINTR:
			case AGAIN_WOULDBLOCK:
				return -1;
			}
			fprintf(stderr, "socket-server : udp (%d) sendto error %s.\n",s->id, strerror(errno));
			drop_udp(ss, s, list, tmp);
			return -1;
		}
		stat_write(ss,s,tmp->sz);
		s->wb_size -= tmp->sz;
		list->head = tmp->next;
		write_buffer_free(ss,tmp);
	}
	list->tail = NULL;

	return -1;
}

static int
send_list(struct socket_server *ss, struct socket *s, struct wb_list *list, struct socket_lock *l, struct socket_message *result) {
	if (s->protocol == PROTOCOL_TCP) {
		return send_list_tcp(ss, s, list, l, result);
	} else {
		return send_list_udp(ss, s, list, result);
	}
}

// list是否还有数据要发送，是的话，返回1，否则返回0
static inline int
list_uncomplete(struct wb_list *s) {
	struct write_buffer *wb = s->head;
	if (wb == NULL)
		return 0;
	
	return (void *)wb->ptr != wb->buffer;
}

// @socket 线程
// 把low list中第一个节点数据移到high list中，注意只移到一个节点数据
// 调用这个接口前，确保hight list已经没有数据了
static void
raise_uncomplete(struct socket * s) {
	struct wb_list *low = &s->low;
	struct write_buffer *tmp = low->head;
	low->head = tmp->next;
	if (low->head == NULL) {
		low->tail = NULL;
	}

	// move head of low list (tmp) to the empty high list
	struct wb_list *high = &s->high;
	assert(high->head == NULL);

	tmp->next = NULL;
	high->head = high->tail = tmp;
}

static inline int
send_buffer_empty(struct socket *s) {
	return (s->high.head == NULL && s->low.head == NULL);
}

/*
	Each socket has two write buffer list, high priority and low priority.

	1. send high list as far as possible.
	2. If high list is empty, try to send low list.
	3. If low list head is uncomplete (send a part before), move the head of low list to empty high list (call raise_uncomplete) .
	4. If two lists are both empty, turn off the event. (call check_close)
 */
// @socket线程 如果有剩余数据没有发送完成，这时候也会返回-1
static int
send_buffer_(struct socket_server *ss, struct socket *s, struct socket_lock *l, struct socket_message *result) {
	assert(!list_uncomplete(&s->low));
	// step 1
	// 优先发送high字段中的数据
	if (send_list(ss,s,&s->high,l,result) == SOCKET_CLOSE) {
		return SOCKET_CLOSE;
	}
	if (s->high.head == NULL) {
		// step 2
		if (s->low.head != NULL) {
			// high 的数据发送完了，则开始发送low list中的数据
			if (send_list(ss,s,&s->low,l,result) == SOCKET_CLOSE) {
				return SOCKET_CLOSE;
			}
			// step 3
			// 如果low list中数据一次性没发送完，则把low list中第一个节点数据移到 high list中
			if (list_uncomplete(&s->low)) {
				raise_uncomplete(s);
				return -1;
			}
			if (s->low.head)
				return -1;
		} 
		// step 4
		// 如果 low 和 high中的数据都发送完成了，则不监听fd是否可写了
		assert(send_buffer_empty(s) && s->wb_size == 0);
		sp_write(ss->event_fd, s->fd, s, false);			

		// close_socket 的时候，可能数据分多次才发送完成，因此这个地方需要处理，执行force_close
		if (s->type == SOCKET_TYPE_HALFCLOSE) {
				force_close(ss, s, l, result);
				return SOCKET_CLOSE;
		}
		if(s->warn_size > 0){
				s->warn_size = 0;
				result->opaque = s->opaque;
				result->id = s->id;
				result->ud = 0;
				result->data = NULL;
				return SOCKET_WARNING;
		}
	}

	return -1;
}

// @socket线程 发送buff中数据到网络上
static int
send_buffer(struct socket_server *ss, struct socket *s, struct socket_lock *l, struct socket_message *result) {
	if (!socket_trylock(l))
		return -1;	// blocked by direct write, send later.

	// 把字段dw_buffer中的数据加入write buff的链表high中
	if (s->dw_buffer) {
		// add direct write buffer before high.head
		struct write_buffer * buf = MALLOC(SIZEOF_TCPBUFFER);
		struct send_object so;
		buf->userobject = send_object_init(ss, &so, (void *)s->dw_buffer, s->dw_size);
		buf->ptr = (char*)so.buffer+s->dw_offset;
		buf->sz = so.sz - s->dw_offset;
		buf->buffer = (void *)s->dw_buffer;
		s->wb_size+=buf->sz;
		if (s->high.head == NULL) {
			s->high.head = s->high.tail = buf;
			buf->next = NULL;
		} else {
			buf->next = s->high.head;
			s->high.head = buf;
		}
		s->dw_buffer = NULL;
	}
	int r = send_buffer_(ss,s,l,result);
	socket_unlock(l);

	return r;
}

// @socket线程 把要发送的数据加到buff list
static struct write_buffer *
append_sendbuffer_(struct socket_server *ss, struct wb_list *s, struct request_send * request, int size) {
	struct write_buffer * buf = MALLOC(size);
	struct send_object so;
	buf->userobject = send_object_init(ss, &so, request->buffer, request->sz);
	buf->ptr = (char*)so.buffer;
	buf->sz = so.sz;
	buf->buffer = request->buffer;
	buf->next = NULL;
	if (s->head == NULL) {
		s->head = s->tail = buf;
	} else {
		assert(s->tail != NULL);
		assert(s->tail->next == NULL);
		s->tail->next = buf;
		s->tail = buf;
	}
	return buf;
}

static inline void
append_sendbuffer_udp(struct socket_server *ss, struct socket *s, int priority, struct request_send * request, const uint8_t udp_address[UDP_ADDRESS_SIZE]) {
	struct wb_list *wl = (priority == PRIORITY_HIGH) ? &s->high : &s->low;
	struct write_buffer *buf = append_sendbuffer_(ss, wl, request, SIZEOF_UDPBUFFER);
	memcpy(buf->udp_address, udp_address, UDP_ADDRESS_SIZE);
	s->wb_size += buf->sz;
}

// @socket线程 把数据加入high list中
static inline void
append_sendbuffer(struct socket_server *ss, struct socket *s, struct request_send * request) {
	struct write_buffer *buf = append_sendbuffer_(ss, &s->high, request, SIZEOF_TCPBUFFER);
	s->wb_size += buf->sz;
}

static inline void
append_sendbuffer_low(struct socket_server *ss,struct socket *s, struct request_send * request) {
	struct write_buffer *buf = append_sendbuffer_(ss, &s->low, request, SIZEOF_TCPBUFFER);
	s->wb_size += buf->sz;
}


/*
	When send a package , we can assign the priority : PRIORITY_HIGH or PRIORITY_LOW

	If socket buffer is empty, write to fd directly.
		If write a part, append the rest part to high list. (Even priority is PRIORITY_LOW)
	Else append package to high (PRIORITY_HIGH) or low (PRIORITY_LOW) list.
 */
// @socket 线程，响应处理来自worker线程的请求 'D' 'P' 'A'
static int
send_socket(struct socket_server *ss, struct request_send * request, struct socket_message *result, int priority, const uint8_t *udp_address) {
	int id = request->id;
	struct socket * s = &ss->slot[HASH_ID(id)];
	struct send_object so;
	send_object_init(ss, &so, request->buffer, request->sz);
	if (s->type == SOCKET_TYPE_INVALID || s->id != id 
		|| s->type == SOCKET_TYPE_HALFCLOSE
		|| s->type == SOCKET_TYPE_PACCEPT) {
		so.free_func(request->buffer);
		return -1;
	}
	if (s->type == SOCKET_TYPE_PLISTEN || s->type == SOCKET_TYPE_LISTEN) {
		fprintf(stderr, "socket-server: write to listen fd %d.\n", id);
		so.free_func(request->buffer);
		return -1;
	}
	if (send_buffer_empty(s) && s->type == SOCKET_TYPE_CONNECTED) {
		if (s->protocol == PROTOCOL_TCP) {
			append_sendbuffer(ss, s, request);	// add to high priority list, even priority == PRIORITY_LOW
		} else {
			// udp
			if (udp_address == NULL) {
				udp_address = s->p.udp_address;
			}
			union sockaddr_all sa;
			socklen_t sasz = udp_socket_address(s, udp_address, &sa);
			if (sasz == 0) {
				// udp type mismatch, just drop it.
				fprintf(stderr, "socket-server: udp socket (%d) type mistach.\n", id);
				so.free_func(request->buffer);
				return -1;
			}
			// 直接发送，如果没有发送成功，则放到buff中去发送
			int n = sendto(s->fd, so.buffer, so.sz, 0, &sa.s, sasz);
			if (n != so.sz) {
				append_sendbuffer_udp(ss,s,priority,request,udp_address);
			} else {
				stat_write(ss,s,n);
				so.free_func(request->buffer);
				return -1;
			}
		}
		// 有数据要写，把fd加入到epoll可写监听事件中
		sp_write(ss->event_fd, s->fd, s, true);
	} else {
		if (s->protocol == PROTOCOL_TCP) {
			if (priority == PRIORITY_LOW) {
				append_sendbuffer_low(ss, s, request);
			} else {
				append_sendbuffer(ss, s, request);
			}
		} else {
			if (udp_address == NULL) {
				udp_address = s->p.udp_address;
			}
			append_sendbuffer_udp(ss,s,priority,request,udp_address);
		}

		// 不为空，表示fd的写事件正在被监听中，则不需要调用sp_write
	}
	if (s->wb_size >= WARNING_SIZE && s->wb_size >= s->warn_size) {
		s->warn_size = s->warn_size == 0 ? WARNING_SIZE *2 : s->warn_size*2;
		result->opaque = s->opaque;
		result->id = s->id;
		result->ud = s->wb_size%1024 == 0 ? s->wb_size/1024 : s->wb_size/1024 + 1;
		result->data = NULL;
		return SOCKET_WARNING;
	}
	return -1;
}

// @socket线程，处理worker线程的请求 'L'
// 主要工作是调用接口new_fd初始化套接字对应的结构体socket信息，这里不会把套接字加入到epoll监听事件中
// 套接字状态从 SOCKET_TYPE_RESERVE --> SOCKET_TYPE_PLISTEN
static int
listen_socket(struct socket_server *ss, struct request_listen * request, struct socket_message *result) {
	int id = request->id;
	int listen_fd = request->fd;
	struct socket *s = new_fd(ss, id, listen_fd, PROTOCOL_TCP, request->opaque, false);
	if (s == NULL) {
		goto _failed;
	}
	s->type = SOCKET_TYPE_PLISTEN;
	return -1;
_failed:
	close(listen_fd);
	result->opaque = request->opaque;
	result->id = id;
	result->ud = 0;
	result->data = "reach skynet socket number limit";
	ss->slot[HASH_ID(id)].type = SOCKET_TYPE_INVALID;

	return SOCKET_ERR;
}

// @socket线程 套接字没有数据要发送到网络上，则返回1
static inline int
nomore_sending_data(struct socket *s) {
	// (s->sending & 0xffff) == 0 表示socket线程没有在发送数据，并且没有请求在发送
	return send_buffer_empty(s) && s->dw_buffer == NULL && (s->sending & 0xffff) == 0;
}

// @socket线程，响应处理来自worker线程的请求 'K'
// 关闭之前，如果buff中有数据，则先发送完成
// 套接字状态设置为 SOCKET_TYPE_HALFCLOSE
static int
close_socket(struct socket_server *ss, struct request_close *request, struct socket_message *result) {
	int id = request->id;
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->type == SOCKET_TYPE_INVALID || s->id != id) {
		result->id = id;
		result->opaque = request->opaque;
		result->ud = 0;
		result->data = NULL;
		return SOCKET_CLOSE;
	}
	struct socket_lock l;
	socket_lock_init(s, &l);
	if (!nomore_sending_data(s)) {
		int type = send_buffer(ss,s,&l,result);
		// type : -1 or SOCKET_WARNING or SOCKET_CLOSE, SOCKET_WARNING means nomore_sending_data
		if (type != -1 && type != SOCKET_WARNING)
			return type;
	}
	if (request->shutdown || nomore_sending_data(s)) {
		force_close(ss,s,&l,result);
		result->id = id;
		result->opaque = request->opaque;
		return SOCKET_CLOSE;
	}
	s->type = SOCKET_TYPE_HALFCLOSE;

	return -1;
}

// @socket线程，处理worker线程的请求 'B'
// 主要工作是调用接口new_fd初始化套接字对应的结构体socket信息，这里会把套接字加入到epoll监听事件中
// 套接字状态从 SOCKET_TYPE_RESERVE --> SOCKET_TYPE_BIND
static int
bind_socket(struct socket_server *ss, struct request_bind *request, struct socket_message *result) {
	int id = request->id;
	result->id = id;
	result->opaque = request->opaque;
	result->ud = 0;
	struct socket *s = new_fd(ss, id, request->fd, PROTOCOL_TCP, request->opaque, true);
	if (s == NULL) {
		result->data = "reach skynet socket number limit";
		return SOCKET_ERR;
	}
	sp_nonblocking(request->fd);
	s->type = SOCKET_TYPE_BIND;
	result->data = "binding";
	return SOCKET_OPEN;
}

// @socket线程，处理worker线程的请求 'S'
// 开启监听或者链接建立完成，即把相应的fd加入事件监控，准备接收数据，修相应的状态的
// SOCKET_TYPE_PLISTEN --> SOCKET_TYPE_LISTEN
// SOCKET_TYPE_PACCEPT --> SOCKET_TYPE_CONNECTED
static int
start_socket(struct socket_server *ss, struct request_start *request, struct socket_message *result) {
	int id = request->id;
	result->id = id;
	result->opaque = request->opaque;
	result->ud = 0;
	result->data = NULL;
	struct socket *s = &ss->slot[HASH_ID(id)];
	if (s->type == SOCKET_TYPE_INVALID || s->id !=id) {
		result->data = "invalid socket";
		return SOCKET_ERR;
	}
	struct socket_lock l;
	socket_lock_init(s, &l);
	if (s->type == SOCKET_TYPE_PACCEPT || s->type == SOCKET_TYPE_PLISTEN) {
		// 把fd加入事件监听，是否有数据可读
		if (sp_add(ss->event_fd, s->fd, s)) {
			force_close(ss, s, &l, result);
			result->data = strerror(errno);
			return SOCKET_ERR;
		}
		// SOCKET_TYPE_PLISTEN --> SOCKET_TYPE_LISTEN
		// SOCKET_TYPE_PACCEPT --> SOCKET_TYPE_CONNECTED
		s->type = (s->type == SOCKET_TYPE_PACCEPT) ? SOCKET_TYPE_CONNECTED : SOCKET_TYPE_LISTEN;
		s->opaque = request->opaque;
		result->data = "start";
		return SOCKET_OPEN;
	} else if (s->type == SOCKET_TYPE_CONNECTED) {
		// todo: maybe we should send a message SOCKET_TRANSFER to s->opaque
		s->opaque = request->opaque;
		result->data = "transfer";
		return SOCKET_OPEN;
	}
	// if s->type == SOCKET_TYPE_HALFCLOSE , SOCKET_CLOSE message will send later
	return -1;
}

// @socket线程，响应处理来自worker线程的请求 'T' 
// 设置套接字选项
static void
setopt_socket(struct socket_server *ss, struct request_setopt *request) {
	int id = request->id;
	struct socket *s = &ss->slot[HASH_ID(id)];
	if (s->type == SOCKET_TYPE_INVALID || s->id !=id) {
		return;
	}
	int v = request->value;
	setsockopt(s->fd, IPPROTO_TCP, request->what, &v, sizeof(v));
}

// @socket 线程 从管道里面读取所有的命令行数据
// 阻塞读取
static void
block_readpipe(int pipefd, void *buffer, int sz) {
	for (;;) {
		int n = read(pipefd, buffer, sz);
		if (n<0) {
			if (errno == EINTR)
				continue;
			fprintf(stderr, "socket-server : read pipe error %s.\n",strerror(errno));
			return;
		}
		// must atomic read from a pipe
		assert(n == sz);
		return;
	}
}

// @socket 线程 检测管道中是否有数据可读取了
// 若有，表示worker线程有向socket线程发送请求
static int
has_cmd(struct socket_server *ss) {
	struct timeval tv = {0,0};
	int retval;

	FD_SET(ss->recvctrl_fd, &ss->rfds);
	
	// 不阻塞检查 recvctrl_fd 是否有数据可以读取
	// 第一个参数设置为最高的fd + 1
	retval = select(ss->recvctrl_fd+1, &ss->rfds, NULL, NULL, &tv);
	if (retval == 1) {
		return 1;
	}
	return 0;
}

// @socket线程，响应处理来自worker线程的请求 'U' 
// 增加新的fd监控读事件
static void
add_udp_socket(struct socket_server *ss, struct request_udp *udp) {
	int id = udp->id;
	int protocol;
	if (udp->family == AF_INET6) {
		protocol = PROTOCOL_UDPv6;
	} else {
		protocol = PROTOCOL_UDP;
	}
	struct socket *ns = new_fd(ss, id, udp->fd, protocol, udp->opaque, true);
	if (ns == NULL) {
		close(udp->fd);
		ss->slot[HASH_ID(id)].type = SOCKET_TYPE_INVALID;
		return;
	}
	ns->type = SOCKET_TYPE_CONNECTED;
	memset(ns->p.udp_address, 0, sizeof(ns->p.udp_address));
}

// @socket线程，响应处理来自worker线程的请求 'C' 
static int
set_udp_address(struct socket_server *ss, struct request_setudp *request, struct socket_message *result) {
	int id = request->id;
	struct socket *s = &ss->slot[HASH_ID(id)];
	if (s->type == SOCKET_TYPE_INVALID || s->id !=id) {
		return -1;
	}
	int type = request->address[0];
	if (type != s->protocol) {
		// protocol mismatch
		result->opaque = s->opaque;
		result->id = s->id;
		result->ud = 0;
		result->data = "protocol mismatch";

		return SOCKET_ERR;
	}
	if (type == PROTOCOL_UDP) {
		memcpy(s->p.udp_address, request->address, 1+2+4);	// 1 type, 2 port, 4 ipv4
	} else {
		memcpy(s->p.udp_address, request->address, 1+2+16);	// 1 type, 2 port, 16 ipv6
	}
	ATOM_DEC(&s->udpconnecting);
	return -1;
}

// @worker线程，每请求socket线程发送数据，调用接口一次
static inline void
inc_sending_ref(struct socket *s, int id) {
	if (s->protocol != PROTOCOL_TCP)
		return;
	for (;;) {
		uint32_t sending = s->sending;
		if ((sending >> 16) == ID_TAG16(id)) {
			if ((sending & 0xffff) == 0xffff) {
				// s->sending may overflow (rarely), so busy waiting here for socket thread dec it. see issue #794
				continue;
			}
			// inc sending only matching the same socket id
			// 字段sending低两个字节增加1
			if (ATOM_CAS(&s->sending, sending, sending + 1))
				return;
			// atom inc failed, retry
		} else {
			// socket id changed, just return
			return;
		}
	}
}

// @socket线程，每处理一次，调用接口一次
static inline void
dec_sending_ref(struct socket_server *ss, int id) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	// Notice: udp may inc sending while type == SOCKET_TYPE_RESERVE
	if (s->id == id && s->protocol == PROTOCOL_TCP) {
		assert((s->sending & 0xffff) != 0);
		ATOM_DEC(&s->sending);
	}
}

// return type
// @socket线程 从pipe中读取worker线程的请求，然后处理
// 参数 result 保存处理请求返回的结果
// 返回值为 SOCKET_DATA SOCKET_CLOSE 等类型
static int
ctrl_cmd(struct socket_server *ss, struct socket_message *result) {
	int fd = ss->recvctrl_fd;
	// the length of message is one byte, so 256+8 buffer size is enough.
	uint8_t buffer[256];
	uint8_t header[2];
	// 从管道读取命令的类型和相应数据长度
	block_readpipe(fd, header, sizeof(header));
	int type = header[0];
	int len = header[1];
	// 从管道读取命令对应的数据
	block_readpipe(fd, buffer, len);
	// ctrl command only exist in local fd, so don't worry about endian.
	switch (type) {
	case 'S':
		return start_socket(ss,(struct request_start *)buffer, result);
	case 'B':
		return bind_socket(ss,(struct request_bind *)buffer, result);
	case 'L':
		return listen_socket(ss,(struct request_listen *)buffer, result);
	case 'K':
		return close_socket(ss,(struct request_close *)buffer, result);
	case 'O':
		return open_socket(ss, (struct request_open *)buffer, result);
	case 'X':
		result->opaque = 0;
		result->id = 0;
		result->ud = 0;
		result->data = NULL;
		return SOCKET_EXIT;
	case 'D':
	case 'P': {
		int priority = (type == 'D') ? PRIORITY_HIGH : PRIORITY_LOW;
		struct request_send * request = (struct request_send *) buffer;
		int ret = send_socket(ss, request, result, priority, NULL);
		dec_sending_ref(ss, request->id);
		return ret;
	}
	case 'A': {
		struct request_send_udp * rsu = (struct request_send_udp *)buffer;
		return send_socket(ss, &rsu->send, result, PRIORITY_HIGH, rsu->address);
	}
	case 'C':
		return set_udp_address(ss, (struct request_setudp *)buffer, result);
	case 'T':
		setopt_socket(ss, (struct request_setopt *)buffer);
		return -1;
	case 'U':
		add_udp_socket(ss, (struct request_udp *)buffer);
		return -1;
	default:
		fprintf(stderr, "socket-server: Unknown ctrl %c.\n",type);
		return -1;
	};

	return -1;
}

// @socket线程 从套接字中读取数据，并把读取的数据放到result，然后给worker线程使用
// return -1 (ignore) when error
static int
forward_message_tcp(struct socket_server *ss, struct socket *s, struct socket_lock *l, struct socket_message * result) {
	int sz = s->p.size;
	char * buffer = MALLOC(sz);
	int n = (int)read(s->fd, buffer, sz);
	if (n<0) {
		FREE(buffer);
		switch(errno) {
		case EINTR:
			break;
		case AGAIN_WOULDBLOCK:
			fprintf(stderr, "socket-server: EAGAIN capture.\n");
			break;
		default:
			// close when error
			force_close(ss, s, l, result);
			result->data = strerror(errno);
			return SOCKET_ERR;
		}
		return -1;
	}
	if (n==0) {
		FREE(buffer);
		// 
		force_close(ss, s, l, result);
		return SOCKET_CLOSE;
	}

	if (s->type == SOCKET_TYPE_HALFCLOSE) {
		// discard recv data
		FREE(buffer);
		return -1;
	}

	stat_read(ss,s,n);

	// 动态调整每次从网络上最多读取的数据大小
	if (n == sz) {
		s->p.size *= 2;
	} else if (sz > MIN_READ_BUFFER && n*2 < sz) {
		s->p.size /= 2;
	}

	result->opaque = s->opaque;
	result->id = s->id;
	result->ud = n;
	result->data = buffer;
	return SOCKET_DATA;
}

static int
gen_udp_address(int protocol, union sockaddr_all *sa, uint8_t * udp_address) {
	int addrsz = 1;
	udp_address[0] = (uint8_t)protocol;
	if (protocol == PROTOCOL_UDP) {
		memcpy(udp_address+addrsz, &sa->v4.sin_port, sizeof(sa->v4.sin_port));
		addrsz += sizeof(sa->v4.sin_port);
		memcpy(udp_address+addrsz, &sa->v4.sin_addr, sizeof(sa->v4.sin_addr));
		addrsz += sizeof(sa->v4.sin_addr);
	} else {
		memcpy(udp_address+addrsz, &sa->v6.sin6_port, sizeof(sa->v6.sin6_port));
		addrsz += sizeof(sa->v6.sin6_port);
		memcpy(udp_address+addrsz, &sa->v6.sin6_addr, sizeof(sa->v6.sin6_addr));
		addrsz += sizeof(sa->v6.sin6_addr);
	}
	return addrsz;
}

static int
forward_message_udp(struct socket_server *ss, struct socket *s, struct socket_lock *l, struct socket_message * result) {
	union sockaddr_all sa;
	socklen_t slen = sizeof(sa);
	int n = recvfrom(s->fd, ss->udpbuffer,MAX_UDP_PACKAGE,0,&sa.s,&slen);
	if (n<0) {
		switch(errno) {
		case EINTR:
		case AGAIN_WOULDBLOCK:
			break;
		default:
			// close when error
			force_close(ss, s, l, result);
			result->data = strerror(errno);
			return SOCKET_ERR;
		}
		return -1;
	}
	stat_read(ss,s,n);

	uint8_t * data;
	if (slen == sizeof(sa.v4)) {
		if (s->protocol != PROTOCOL_UDP)
			return -1;
		data = MALLOC(n + 1 + 2 + 4);
		gen_udp_address(PROTOCOL_UDP, &sa, data + n);
	} else {
		if (s->protocol != PROTOCOL_UDPv6)
			return -1;
		data = MALLOC(n + 1 + 2 + 16);
		gen_udp_address(PROTOCOL_UDPv6, &sa, data + n);
	}
	memcpy(data, ss->udpbuffer, n);

	result->opaque = s->opaque;
	result->id = s->id;
	result->ud = n;
	result->data = (char *)data;

	return SOCKET_UDP;
}

// @socket线程 当前请求连接的连接成功时候，调用这个接口，其工作是把套接字状态
// SOCKET_TYPE_CONNECTING --> SOCKET_TYPE_CONNECTED
// 此时设置套接字结构体相关的信息，比如对方的ip地址
// 构造result，用来通知worker线程connet成功了
static int
report_connect(struct socket_server *ss, struct socket *s, struct socket_lock *l, struct socket_message *result) {
	int error;
	socklen_t len = sizeof(error);  
	int code = getsockopt(s->fd, SOL_SOCKET, SO_ERROR, &error, &len);  
	if (code < 0 || error) {  
		force_close(ss,s,l, result);
		if (code >= 0)
			result->data = strerror(error);
		else
			result->data = strerror(errno);
		return SOCKET_ERR;
	} else {
		s->type = SOCKET_TYPE_CONNECTED;
		result->opaque = s->opaque;
		result->id = s->id;
		result->ud = 0;
		if (nomore_sending_data(s)) {
			sp_write(ss->event_fd, s->fd, s, false);
		}
		union sockaddr_all u;
		socklen_t slen = sizeof(u);
		if (getpeername(s->fd, &u.s, &slen) == 0) {
			void * sin_addr = (u.s.sa_family == AF_INET) ? (void*)&u.v4.sin_addr : (void *)&u.v6.sin6_addr;
			if (inet_ntop(u.s.sa_family, sin_addr, ss->buffer, sizeof(ss->buffer))) {
				result->data = ss->buffer;
				return SOCKET_OPEN;
			}
		}
		result->data = NULL;
		return SOCKET_OPEN;
	}
}

// 把相应的ip地址和端口号以字符串的形式写入buffer中
static int
getname(union sockaddr_all *u, char *buffer, size_t sz) {
	char tmp[INET6_ADDRSTRLEN];
	void * sin_addr = (u->s.sa_family == AF_INET) ? (void*)&u->v4.sin_addr : (void *)&u->v6.sin6_addr;
	int sin_port = ntohs((u->s.sa_family == AF_INET) ? u->v4.sin_port : u->v6.sin6_port);
	if (inet_ntop(u->s.sa_family, sin_addr, tmp, sizeof(tmp))) {
		snprintf(buffer, sz, "%s:%d", tmp, sin_port);
		return 1;
	} else {
		buffer[0] = '\0';
		return 0;
	}
}

// @socket线程 监听的套接字收到新的连接时候，调用这个接口，其主要工作是：
// 调用accept接收先的连接，然后调用new_fd创建对应的保存套接字信息结构体socket
// return 0 when failed, or -1 when file limit
static int
report_accept(struct socket_server *ss, struct socket *s, struct socket_message *result) {
	union sockaddr_all u;
	socklen_t len = sizeof(u);
	int client_fd = accept(s->fd, &u.s, &len);
	if (client_fd < 0) {
		if (errno == EMFILE || errno == ENFILE) {
			result->opaque = s->opaque;
			result->id = s->id;
			result->ud = 0;
			result->data = strerror(errno);
			return -1;
		} else {
			return 0;
		}
	}
	int id = reserve_id(ss);
	if (id < 0) {
		close(client_fd);
		return 0;
	}
	socket_keepalive(client_fd);
	sp_nonblocking(client_fd);
	struct socket *ns = new_fd(ss, id, client_fd, PROTOCOL_TCP, s->opaque, false);
	if (ns == NULL) {
		close(client_fd);
		return 0;
	}
	// accept new one connection
	stat_read(ss,s,1);

	ns->type = SOCKET_TYPE_PACCEPT;
	result->opaque = s->opaque;
	result->id = s->id;
	result->ud = id;
	result->data = NULL;

	if (getname(&u, ss->buffer, sizeof(ss->buffer))) {
		result->data = ss->buffer;
	}

	return 1;
}

static inline void 
clear_closed_event(struct socket_server *ss, struct socket_message * result, int type) {
	if (type == SOCKET_CLOSE || type == SOCKET_ERR) {
		int id = result->id;
		int i;
		for (i=ss->event_index; i<ss->event_n; i++) {
			struct event *e = &ss->ev[i];
			struct socket *s = e->s;
			if (s) {
				if (s->type == SOCKET_TYPE_INVALID && s->id == id) {
					e->s = NULL;
					break;
				}
			}
		}
	}
}

// @socket线程 接口skynet_socket_poll调用这个接口
// return type
int 
socket_server_poll(struct socket_server *ss, struct socket_message * result, int * more) {
	for (;;) {
		// 处理来自woker线程的请求，从pipe中读取
		// 优先处理来自woker线程的请求，处理完成后，在跑后面的逻辑
		if (ss->checkctrl) {
			if (has_cmd(ss)) {
				// 检查是否有worker线程请求，若有，则处理，并且只处理一个请求，然后返回
				// 再下一次调用的时候再处理
				int type = ctrl_cmd(ss, result);
				if (type != -1) {
					clear_closed_event(ss, result, type);
					return type;
				} else
					continue;
			} else {
				// 处理所有来自worker线程请求后，设置标识，下面循环处理网络上的读写事件
				// 即下面的逻辑
				ss->checkctrl = 0;
			}
		}

		// 等待网络上的读写事件
		if (ss->event_index == ss->event_n) {
			// 初始的时候，或者所有的事件处理完后，调用epoll_wait等待相应的事件到来
			// sp_wait返回的值，为epoll_wait的返回值，即触发事件的数量
			ss->event_n = sp_wait(ss->event_fd, ss->ev, MAX_EVENT);
			ss->checkctrl = 1;
			if (more) {
				*more = 0;
			}
			ss->event_index = 0;
			if (ss->event_n <= 0) {
				ss->event_n = 0;
				// epoll_wait 被信号中断了，重新epoll_wait
				if (errno == EINTR) {
					continue;
				}
				return -1;
			}
		}

		// 开始处理每一个可读写的事件，每次socket_server_poll调用只处理一个事件
		struct event *e = &ss->ev[ss->event_index++];
		struct socket *s = e->s;
		if (s == NULL) {
			// dispatch pipe message at beginning
			// 因为在socket_server_create中，把recvctrl_fd 也加入到poll event监听了
			// 加入监听的原因，防止没有网络相关的事件，但是有worker线程的请求，这时候也不能阻塞的
			continue;
		}
		struct socket_lock l;
		socket_lock_init(s, &l);
		switch (s->type) {
		case SOCKET_TYPE_CONNECTING:
			// 正在等待连接的套接字，等到可写的事件了，表示连接成功了
			return report_connect(ss, s, &l, result);
		case SOCKET_TYPE_LISTEN: {
			// 监听的套接字，收到新的连接调用
			int ok = report_accept(ss, s, result);
			if (ok > 0) {
				return SOCKET_ACCEPT;
			} if (ok < 0 ) {
				return SOCKET_ERR;
			}
			// when ok == 0, retry
			break;
		}
		case SOCKET_TYPE_INVALID:
			fprintf(stderr, "socket-server: invalid socket\n");
			break;
		default:
			if (e->read) {
				// 有数据可读
				int type;
				if (s->protocol == PROTOCOL_TCP) {
					type = forward_message_tcp(ss, s, &l, result);
				} else {
					type = forward_message_udp(ss, s, &l, result);
					if (type == SOCKET_UDP) {
						// try read again
						--ss->event_index;
						return SOCKET_UDP;
					}
				}
				if (e->write && type != SOCKET_CLOSE && type != SOCKET_ERR) {
					// Try to dispatch write message next step if write flag set.
					// 本事件不仅有读事件，还有写事件，则下次调用还需要这个event，即处理它的写事件
					e->read = false;
					--ss->event_index;
				}
				if (type == -1)
					break;				
				return type;
			}
			if (e->write) {
				int type = send_buffer(ss, s, &l, result);
				if (type == -1)
					break;
				return type;
			}
			if (e->error) {
				// close when error
				// 处理错误，把错误信息返会给worker线程，同时close相应的套接字
				int error;
				socklen_t len = sizeof(error);  
				int code = getsockopt(s->fd, SOL_SOCKET, SO_ERROR, &error, &len);  
				const char * err = NULL;
				if (code < 0) {
					err = strerror(errno);
				} else if (error != 0) {
					err = strerror(error);
				} else {
					err = "Unknown error";
				}
				force_close(ss, s, &l, result);
				result->data = (char *)err;
				return SOCKET_ERR;
			}
			if(e->eof) {
				force_close(ss, s, &l, result);
				return SOCKET_CLOSE;
			}
			break;
		}
	}
}

// @worker线程，向pipe中写入数据
// 即worker线程发请求信息写入到pip中，等待socket线程处理
static void
send_request(struct socket_server *ss, struct request_package *request, char type, int len) {
	request->header[6] = (uint8_t)type;
	request->header[7] = (uint8_t)len;
	for (;;) {
		ssize_t n = write(ss->sendctrl_fd, &request->header[6], len+2);
		if (n<0) {
			if (errno != EINTR) {
				fprintf(stderr, "socket-server : send ctrl command error %s.\n", strerror(errno));
			}
			continue;
		}
		assert(n == len+2);
		return;
	}
}

// @worker线程，构造请求连接的request_package
static int
open_request(struct socket_server *ss, struct request_package *req, uintptr_t opaque, const char *addr, int port) {
	int len = strlen(addr);
	if (len + sizeof(req->u.open) >= 256) {
		fprintf(stderr, "socket-server : Invalid addr %s.\n",addr);
		return -1;
	}
	int id = reserve_id(ss);
	if (id < 0)
		return -1;
	req->u.open.opaque = opaque;
	req->u.open.id = id;
	req->u.open.port = port;
	memcpy(req->u.open.host, addr, len);
	req->u.open.host[len] = '\0';

	return len;
}

// @worker线程，放出请求connect指定的host和port，然后socket线程去处理连接过程
int 
socket_server_connect(struct socket_server *ss, uintptr_t opaque, const char * addr, int port) {
	struct request_package request;
	int len = open_request(ss, &request, opaque, addr, port);
	if (len < 0)
		return -1;
	send_request(ss, &request, 'O', sizeof(request.u.open) + len);
	return request.u.open.id;
}

static inline int
can_direct_write(struct socket *s, int id) {
	return s->id == id && nomore_sending_data(s) && s->type == SOCKET_TYPE_CONNECTED && s->udpconnecting == 0;
}

// @worker线程，请求向指定套接字发送数据
// return -1 when error, 0 when success
int 
socket_server_send(struct socket_server *ss, int id, const void * buffer, int sz) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		free_buffer(ss, buffer, sz);
		return -1;
	}

	struct socket_lock l;
	socket_lock_init(s, &l);

	if (can_direct_write(s,id) && socket_trylock(&l)) {
		// may be we can send directly, double check
		if (can_direct_write(s,id)) {
			// send directly
			// 如果socket线程没有在写数据，并且buff list没有数据要发送，即socket线程完全空闲，
			// 则worker线程直接，往网络上写数据
			struct send_object so;
			send_object_init(ss, &so, (void *)buffer, sz);
			ssize_t n;
			if (s->protocol == PROTOCOL_TCP) {
				// 因为fd都是非阻塞的，因此这个地方不会阻塞工作线程的
				n = write(s->fd, so.buffer, so.sz);
			} else {
				union sockaddr_all sa;
				socklen_t sasz = udp_socket_address(s, s->p.udp_address, &sa);
				if (sasz == 0) {
					fprintf(stderr, "socket-server : set udp (%d) address first.\n", id);
					socket_unlock(&l);
					so.free_func((void *)buffer);
					return -1;
				}
				n = sendto(s->fd, so.buffer, so.sz, 0, &sa.s, sasz);
			}
			if (n<0) {
				// ignore error, let socket thread try again
				n = 0;
			}
			stat_write(ss,s,n);
			if (n == so.sz) {
				// write done
				socket_unlock(&l);
				so.free_func((void *)buffer);
				return 0;
			}
			// write failed, put buffer into s->dw_* , and let socket thread send it. see send_buffer()
			s->dw_buffer = buffer;
			s->dw_size = sz;
			s->dw_offset = n;

			sp_write(ss->event_fd, s->fd, s, true);

			socket_unlock(&l);
			return 0;
		}
		socket_unlock(&l);
	}

	inc_sending_ref(s, id);

	struct request_package request;
	request.u.send.id = id;
	request.u.send.sz = sz;
	request.u.send.buffer = (char *)buffer;

	send_request(ss, &request, 'D', sizeof(request.u.send));
	return 0;
}

// @worker线程，请求发送low优先级的数据
// return -1 when error, 0 when success
int 
socket_server_send_lowpriority(struct socket_server *ss, int id, const void * buffer, int sz) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		free_buffer(ss, buffer, sz);
		return -1;
	}

	inc_sending_ref(s, id);

	struct request_package request;
	request.u.send.id = id;
	request.u.send.sz = sz;
	request.u.send.buffer = (char *)buffer;

	send_request(ss, &request, 'P', sizeof(request.u.send));
	return 0;
}

// 在timer线程退出的时候调用，用来唤醒 sokcet线程，在timer线程中调用
void
socket_server_exit(struct socket_server *ss) {
	struct request_package request;
	send_request(ss, &request, 'X', 0);
}

// @worker线程，情况socket线程关闭连接，不强制关闭
void
socket_server_close(struct socket_server *ss, uintptr_t opaque, int id) {
	struct request_package request;
	request.u.close.id = id;
	request.u.close.shutdown = 0;
	request.u.close.opaque = opaque;
	send_request(ss, &request, 'K', sizeof(request.u.close));
}

// @worker线程，强求socket线程关闭连接，强制关闭
void
socket_server_shutdown(struct socket_server *ss, uintptr_t opaque, int id) {
	struct request_package request;
	request.u.close.id = id;
	request.u.close.shutdown = 1;
	request.u.close.opaque = opaque;
	send_request(ss, &request, 'K', sizeof(request.u.close));
}

// return -1 means failed
// or return AF_INET or AF_INET6
// 工作线程中执行
// 参数 protocol 类型为 IPPROTO_TCP 或者是 IPPROTO_UDP
// 接口工作：创建网络套接字、绑定到相应的地址
static int
do_bind(const char *host, int port, int protocol, int *family) {
	int fd;
	int status;
	int reuse = 1;
	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	char portstr[16];
	if (host == NULL || host[0] == 0) {
		host = "0.0.0.0";	// INADDR_ANY
	}
	sprintf(portstr, "%d", port);
	memset( &ai_hints, 0, sizeof( ai_hints ) );
	ai_hints.ai_family = AF_UNSPEC;
	if (protocol == IPPROTO_TCP) {
		ai_hints.ai_socktype = SOCK_STREAM;
	} else {
		assert(protocol == IPPROTO_UDP);
		ai_hints.ai_socktype = SOCK_DGRAM;
	}
	ai_hints.ai_protocol = protocol;

	// 获取host和port对应的网络信息
	status = getaddrinfo( host, portstr, &ai_hints, &ai_list );
	if ( status != 0 ) {
		return -1;
	}
	*family = ai_list->ai_family;
	fd = socket(*family, ai_list->ai_socktype, 0);
	if (fd < 0) {
		goto _failed_fd;
	}
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(int))==-1) {
		goto _failed;
	}
	status = bind(fd, (struct sockaddr *)ai_list->ai_addr, ai_list->ai_addrlen);
	if (status != 0)
		goto _failed;

	freeaddrinfo( ai_list );
	return fd;
_failed:
	close(fd);
_failed_fd:
	freeaddrinfo( ai_list );
	return -1;
}

// @worker线程，返回相应监听的fd
static int
do_listen(const char * host, int port, int backlog) {
	int family = 0;
	int listen_fd = do_bind(host, port, IPPROTO_TCP, &family);
	if (listen_fd < 0) {
		return -1;
	}
	if (listen(listen_fd, backlog) == -1) {
		close(listen_fd);
		return -1;
	}
	return listen_fd;
}

// @worker线程，请求创建一个监听套接字调用这个接口
// 在worker线程创建好创建好套接字相关的信息
// 请求socket线程做的工作是，初始化对应的结构体和监听相关事件
// 参数 opaque 通常是服务对应的handle
int 
socket_server_listen(struct socket_server *ss, uintptr_t opaque, const char * addr, int port, int backlog) {
	int fd = do_listen(addr, port, backlog);
	if (fd < 0) {
		return -1;
	}
	struct request_package request;
	int id = reserve_id(ss);
	if (id < 0) {
		close(fd);
		return id;
	}
	request.u.listen.opaque = opaque;
	request.u.listen.id = id;
	request.u.listen.fd = fd;
	send_request(ss, &request, 'L', sizeof(request.u.listen));
	return id;
}

// @worker线程，请求bind和监听fd
int
socket_server_bind(struct socket_server *ss, uintptr_t opaque, int fd) {
	struct request_package request;
	int id = reserve_id(ss);
	if (id < 0)
		return -1;
	request.u.bind.opaque = opaque;
	request.u.bind.id = id;
	request.u.bind.fd = fd;
	send_request(ss, &request, 'B', sizeof(request.u.bind));
	return id;
}

// @worker 线程，请求开始监听
void 
socket_server_start(struct socket_server *ss, uintptr_t opaque, int id) {
	struct request_package request;
	request.u.start.id = id;
	request.u.start.opaque = opaque;
	send_request(ss, &request, 'S', sizeof(request.u.start));
}

void
socket_server_nodelay(struct socket_server *ss, int id) {
	struct request_package request;
	request.u.setopt.id = id;
	request.u.setopt.what = TCP_NODELAY;
	request.u.setopt.value = 1;
	send_request(ss, &request, 'T', sizeof(request.u.setopt));
}

void 
socket_server_userobject(struct socket_server *ss, struct socket_object_interface *soi) {
	ss->soi = *soi;
}

// UDP

int 
socket_server_udp(struct socket_server *ss, uintptr_t opaque, const char * addr, int port) {
	int fd;
	int family;
	if (port != 0 || addr != NULL) {
		// bind
		fd = do_bind(addr, port, IPPROTO_UDP, &family);
		if (fd < 0) {
			return -1;
		}
	} else {
		family = AF_INET;
		fd = socket(family, SOCK_DGRAM, 0);
		if (fd < 0) {
			return -1;
		}
	}
	sp_nonblocking(fd);

	int id = reserve_id(ss);
	if (id < 0) {
		close(fd);
		return -1;
	}
	struct request_package request;
	request.u.udp.id = id;
	request.u.udp.fd = fd;
	request.u.udp.opaque = opaque;
	request.u.udp.family = family;

	send_request(ss, &request, 'U', sizeof(request.u.udp));	
	return id;
}

int 
socket_server_udp_send(struct socket_server *ss, int id, const struct socket_udp_address *addr, const void *buffer, int sz) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		free_buffer(ss, buffer, sz);
		return -1;
	}

	const uint8_t *udp_address = (const uint8_t *)addr;
	int addrsz;
	switch (udp_address[0]) {
	case PROTOCOL_UDP:
		addrsz = 1+2+4;		// 1 type, 2 port, 4 ipv4
		break;
	case PROTOCOL_UDPv6:
		addrsz = 1+2+16;	// 1 type, 2 port, 16 ipv6
		break;
	default:
		free_buffer(ss, buffer, sz);
		return -1;
	}

	struct socket_lock l;
	socket_lock_init(s, &l);

	if (can_direct_write(s,id) && socket_trylock(&l)) {
		// may be we can send directly, double check
		if (can_direct_write(s,id)) {
			// send directly
			struct send_object so;
			send_object_init(ss, &so, (void *)buffer, sz);
			union sockaddr_all sa;
			socklen_t sasz = udp_socket_address(s, udp_address, &sa);
			if (sasz == 0) {
				socket_unlock(&l);
				so.free_func((void *)buffer);
				return -1;
			}
			int n = sendto(s->fd, so.buffer, so.sz, 0, &sa.s, sasz);
			if (n >= 0) {
				// sendto succ
				stat_write(ss,s,n);
				socket_unlock(&l);
				so.free_func((void *)buffer);
				return 0;
			}
		}
		socket_unlock(&l);
		// let socket thread try again, udp doesn't care the order
	}

	struct request_package request;
	request.u.send_udp.send.id = id;
	request.u.send_udp.send.sz = sz;
	request.u.send_udp.send.buffer = (char *)buffer;

	memcpy(request.u.send_udp.address, udp_address, addrsz);

	send_request(ss, &request, 'A', sizeof(request.u.send_udp.send)+addrsz);
	return 0;
}

int
socket_server_udp_connect(struct socket_server *ss, int id, const char * addr, int port) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		return -1;
	}
	struct socket_lock l;
	socket_lock_init(s, &l);
	socket_lock(&l);
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		socket_unlock(&l);
		return -1;
	}
	ATOM_INC(&s->udpconnecting);
	socket_unlock(&l);

	int status;
	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	char portstr[16];
	sprintf(portstr, "%d", port);
	memset( &ai_hints, 0, sizeof( ai_hints ) );
	ai_hints.ai_family = AF_UNSPEC;
	ai_hints.ai_socktype = SOCK_DGRAM;
	ai_hints.ai_protocol = IPPROTO_UDP;

	status = getaddrinfo(addr, portstr, &ai_hints, &ai_list );
	if ( status != 0 ) {
		return -1;
	}
	struct request_package request;
	request.u.set_udp.id = id;
	int protocol;

	if (ai_list->ai_family == AF_INET) {
		protocol = PROTOCOL_UDP;
	} else if (ai_list->ai_family == AF_INET6) {
		protocol = PROTOCOL_UDPv6;
	} else {
		freeaddrinfo( ai_list );
		return -1;
	}

	int addrsz = gen_udp_address(protocol, (union sockaddr_all *)ai_list->ai_addr, request.u.set_udp.address);

	freeaddrinfo( ai_list );

	send_request(ss, &request, 'C', sizeof(request.u.set_udp) - sizeof(request.u.set_udp.address) +addrsz);

	return 0;
}

const struct socket_udp_address *
socket_server_udp_address(struct socket_server *ss, struct socket_message *msg, int *addrsz) {
	uint8_t * address = (uint8_t *)(msg->data + msg->ud);
	int type = address[0];
	switch(type) {
	case PROTOCOL_UDP:
		*addrsz = 1+2+4;
		break;
	case PROTOCOL_UDPv6:
		*addrsz = 1+2+16;
		break;
	default:
		return NULL;
	}
	return (const struct socket_udp_address *)address;
}


struct socket_info *
socket_info_create(struct socket_info *last) {
	struct socket_info *si = skynet_malloc(sizeof(*si));
	memset(si, 0 , sizeof(*si));
	si->next = last;
	return si;
}

void
socket_info_release(struct socket_info *si) {
	while (si) {
		struct socket_info *temp = si;
		si = si->next;
		skynet_free(temp);
	}
}

static int
query_info(struct socket *s, struct socket_info *si) {
	union sockaddr_all u;
	socklen_t slen = sizeof(u);
	switch (s->type) {
	case SOCKET_TYPE_BIND:
		si->type = SOCKET_INFO_BIND;
		si->name[0] = '\0';
		break;
	case SOCKET_TYPE_LISTEN:
		si->type = SOCKET_INFO_LISTEN;
		if (getsockname(s->fd, &u.s, &slen) == 0) {
			getname(&u, si->name, sizeof(si->name));
		}
		break;
	case SOCKET_TYPE_CONNECTED:
		if (s->protocol == PROTOCOL_TCP) {
			si->type = SOCKET_INFO_TCP;
			if (getpeername(s->fd, &u.s, &slen) == 0) {
				getname(&u, si->name, sizeof(si->name));
			}
		} else {
			si->type = SOCKET_INFO_UDP;
			if (udp_socket_address(s, s->p.udp_address, &u)) {
				getname(&u, si->name, sizeof(si->name));
			}
		}
		break;
	default:
		return 0;
	}
	si->id = s->id;
	si->opaque = (uint64_t)s->opaque;
	si->read = s->stat.read;
	si->write = s->stat.write;
	si->rtime = s->stat.rtime;
	si->wtime = s->stat.wtime;
	si->wbuffer = s->wb_size;

	return 1;
}

struct socket_info *
socket_server_info(struct socket_server *ss) {
	int i;
	struct socket_info * si = NULL;
	for (i=0;i<MAX_SOCKET;i++) {
		struct socket * s = &ss->slot[i];
		int id = s->id;
		struct socket_info temp;
		if (query_info(s, &temp) && s->id == id) {
			// socket_server_info may call in different thread, so check socket id again
			si = socket_info_create(si);
			temp.next = si->next;
			*si = temp;
		}
	}
	return si;
}
