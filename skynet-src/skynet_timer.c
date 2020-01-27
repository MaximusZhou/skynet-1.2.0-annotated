#include "skynet.h"

#include "skynet_timer.h"
#include "skynet_mq.h"
#include "skynet_server.h"
#include "skynet_handle.h"
#include "spinlock.h"

#include <time.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#if defined(__APPLE__)
#include <AvailabilityMacros.h>
#include <sys/time.h>
#include <mach/task.h>
#include <mach/mach.h>
#endif

typedef void (*timer_execute_func)(void *ud,void *arg);

#define TIME_NEAR_SHIFT 8
#define TIME_NEAR (1 << TIME_NEAR_SHIFT) // 每一个层级的，轮盘的大小
#define TIME_LEVEL_SHIFT 6
#define TIME_LEVEL (1 << TIME_LEVEL_SHIFT)
#define TIME_NEAR_MASK (TIME_NEAR-1)
#define TIME_LEVEL_MASK (TIME_LEVEL-1)

struct timer_event {
	uint32_t handle;  // 服务对应的handle
	int session;      // 保存是服务那个session来增加定时器的，唯一标识一条消息
};

// 相同时间超时定时器组成的链表节点
struct timer_node {
	struct timer_node *next;
	uint32_t expire;
};

struct link_list {
	struct timer_node head; // 其next指向链表第一个timer_node
	struct timer_node *tail;
};

struct timer {
	// 定时最大设置超时时间为，单位为10毫秒
	// TIME_NEAR + TIME_NEAR * TIME_LEVEL + TIME_NEAR * TIME_LEVEL * TIME_LEVEL  + 
	// TIME_NEAR * TIME_LEVEL * TIME_LEVEL * TIME_LEVEL +
	// TIME_NEAR * TIME_LEVEL * TIME_LEVEL * TIME_LEVEL * TIME_LEVEL
	struct link_list near[TIME_NEAR]; // 第一层轮盘，每个元素对应一个链表，第一层轮盘大小为TIME_NEAR
	struct link_list t[4][TIME_LEVEL]; // 其他层的轮盘链表信息，一个4层，每层轮盘大小为TIME_LEVEL
	struct spinlock lock; // 工作线程和timer线程访问全局变量TI都需要加锁

	// 初始化值为0，每10毫秒累加1，是定时器本身维护的一个时间，增加的定时器时候，都是相对这个时间来设置的
	uint32_t time; 
	uint32_t starttime; // skynet启动的时候时间，保存是秒
	uint64_t current; // 保存skynet启动以来，运行的厘秒数

 	// 单位当前系统时间的厘秒数，skynet当前轮询更新的时候，timer线程轮询更新这个字段的值
	uint64_t current_point;
};

static struct timer * TI = NULL;

// 删除某层中的slot，并且返回
static inline struct timer_node *
link_clear(struct link_list *list) {
	struct timer_node * ret = list->head.next;
	list->head.next = 0;
	list->tail = &(list->head);

	return ret;
}

static inline void
link(struct link_list *list,struct timer_node *node) {
	list->tail->next = node;
	list->tail = node;
	node->next=0;
}

static void
add_node(struct timer *T,struct timer_node *node) {
	uint32_t time=node->expire;
	uint32_t current_time=T->time;
	
	if ((time|TIME_NEAR_MASK)==(current_time|TIME_NEAR_MASK)) {
		// 表示在第一层轮，直接加到相应的链表中
		link(&T->near[time&TIME_NEAR_MASK],node);
	} else {
		int i;
		// 查找在那一层
		uint32_t mask=TIME_NEAR << TIME_LEVEL_SHIFT; // 计算第二层轮盘的mask
		for (i=0;i<3;i++) {
			if ((time|(mask-1))==(current_time|(mask-1))) {
				break;
			}
			mask <<= TIME_LEVEL_SHIFT; // 计算下一层轮盘的mask
		}

		// 增加到相应层的轮盘中
		link(&T->t[i][((time>>(TIME_NEAR_SHIFT + i*TIME_LEVEL_SHIFT)) & TIME_LEVEL_MASK)],node);	
	}
}

// 在工作线程中调用，增加一个定时器
static void
timer_add(struct timer *T,void *arg,size_t sz,int time) {
	// 分配的内存空间包含额外的参数空间，node后面紧跟额外的参数信息
	struct timer_node *node = (struct timer_node *)skynet_malloc(sizeof(*node)+sz);
	memcpy(node+1,arg,sz);

	SPIN_LOCK(T);

		node->expire=time+T->time; // 计算相对于定时器当前时间来说，过期的时间
		add_node(T,node);

	SPIN_UNLOCK(T);
}

// 移到lvevel层级的，slot的下标为idx的链表
static void
move_list(struct timer *T, int level, int idx) {
	struct timer_node *current = link_clear(&T->t[level][idx]);
	// 把要移到的链表重新加入到定时器中
	while (current) {
		struct timer_node *temp=current->next;
		add_node(T,current);
		current=temp;
	}
}

static void
timer_shift(struct timer *T) {
	int mask = TIME_NEAR;
	uint32_t ct = ++T->time;
	if (ct == 0) {
		move_list(T, 3, 0);
	} else {
		uint32_t time = ct >> TIME_NEAR_SHIFT;
		int i=0;

		// 当 ct & (mask - 1) == 0 说明触发了层级调整，上一层需要移到下一层了
		while ((ct & (mask-1))==0) {
			int idx=time & TIME_LEVEL_MASK;
			if (idx!=0) {
				// 表示找到相应的触发层级的slot
				// 注意每次最多触发一个层级的一个slot调整
				move_list(T, i, idx);
				break;				
			}
			mask <<= TIME_LEVEL_SHIFT;
			time >>= TIME_LEVEL_SHIFT;
			++i;
		}
	}
}

// 执行相应的定时器的回调函数逻辑，即向次级消息队列中push一个消息
// 一次处理一条都到期的链表
static inline void
dispatch_list(struct timer_node *current) {
	do {
		struct timer_event * event = (struct timer_event *)(current+1);
		struct skynet_message message;
		message.source = 0;
		message.session = event->session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		skynet_context_push(event->handle, &message);
		
		struct timer_node * temp = current;
		current=current->next;
		skynet_free(temp);	
	} while (current);
}

// 检测到时间到的定时器，执行相应的回调
static inline void
timer_execute(struct timer *T) {
	int idx = T->time & TIME_NEAR_MASK;
	
	while (T->near[idx].head.next) {
		struct timer_node *current = link_clear(&T->near[idx]);
		SPIN_UNLOCK(T);
		// dispatch_list don't need lock T
		dispatch_list(current);
		SPIN_LOCK(T);
	}
}

// timer线程会轮询调用这个接口，每隔n 厘秒，就调用这个接口n次
// timer线程中调用
// 即timer定时器的粒度为厘秒，即10毫秒
static void 
timer_update(struct timer *T) {
	SPIN_LOCK(T);

	// try to dispatch timeout 0 (rare condition)
	timer_execute(T);

	// shift time first, and then dispatch timer message
	timer_shift(T);

	timer_execute(T);

	SPIN_UNLOCK(T);
}

static struct timer *
timer_create_timer() {
	struct timer *r=(struct timer *)skynet_malloc(sizeof(struct timer));
	memset(r,0,sizeof(*r));

	int i,j;

	for (i=0;i<TIME_NEAR;i++) {
		link_clear(&r->near[i]);
	}

	for (i=0;i<4;i++) {
		for (j=0;j<TIME_LEVEL;j++) {
			link_clear(&r->t[i][j]);
		}
	}

	SPIN_INIT(r)

	r->current = 0;

	return r;
}

// 该接口在工作线程中被接口cmd_timeout调用
// 用来增加一个应用层的定时器
// 定时器触发的时候，应用层要调用的函数和参数，由应用层自己去管理和处理
// 定时器触发的时候，唯一做的工作是向相应服务的次级消息队列push一个PTYPE_RESPONSE的消息
// 然后根据消息的session，应用层自己找到相应的回调函数和参数
int
skynet_timeout(uint32_t handle, int time, int session) {
	if (time <= 0) {
		struct skynet_message message;
		message.source = 0;
		message.session = session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		// timeout <= 0，则直接push到次级消息队列中
		if (skynet_context_push(handle, &message)) {
			return -1;
		}
	} else {
		struct timer_event event;
		event.handle = handle;
		event.session = session;
		timer_add(TI, &event, sizeof(event), time);
	}

	return session;
}

// centisecond: 1/100 second
static void
systime(uint32_t *sec, uint32_t *cs) {
#if !defined(__APPLE__) || defined(AVAILABLE_MAC_OS_X_VERSION_10_12_AND_LATER)
	struct timespec ti;
	clock_gettime(CLOCK_REALTIME, &ti);
	*sec = (uint32_t)ti.tv_sec;
	*cs = (uint32_t)(ti.tv_nsec / 10000000);
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	*sec = tv.tv_sec;
	*cs = tv.tv_usec / 10000;
#endif
}

// 调用系统接口，返回厘秒，则秒/100
static uint64_t
gettime() {
	uint64_t t;
#if !defined(__APPLE__) || defined(AVAILABLE_MAC_OS_X_VERSION_10_12_AND_LATER)
	struct timespec ti;
	clock_gettime(CLOCK_MONOTONIC, &ti);
	t = (uint64_t)ti.tv_sec * 100;
	t += ti.tv_nsec / 10000000;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	t = (uint64_t)tv.tv_sec * 100;
	t += tv.tv_usec / 10000;
#endif
	return t;
}

// timer线程中轮询调用这个接口
// 更新current_point 和 current 字段的值
void
skynet_updatetime(void) {
	uint64_t cp = gettime();
	if(cp < TI->current_point) {
		skynet_error(NULL, "time diff error: change from %lld to %lld", cp, TI->current_point);
		TI->current_point = cp;
	} else if (cp != TI->current_point) {
		uint32_t diff = (uint32_t)(cp - TI->current_point);
		TI->current_point = cp;
		TI->current += diff;
		int i;
		for (i=0;i<diff;i++) {
			timer_update(TI);
		}
	}
}

uint32_t
skynet_starttime(void) {
	return TI->starttime;
}

// 返回的skynet启动以来，经过的厘秒数
uint64_t 
skynet_now(void) {
	return TI->current;
}

// 服务器启动的时候调用，初始化定时器管理模块
void 
skynet_timer_init(void) {
	TI = timer_create_timer();
	uint32_t current = 0;
	systime(&TI->starttime, &current);
	TI->current = current;
	TI->current_point = gettime();
}

// for profile

#define NANOSEC 1000000000
#define MICROSEC 1000000

uint64_t
skynet_thread_time(void) {
#if  !defined(__APPLE__) || defined(AVAILABLE_MAC_OS_X_VERSION_10_12_AND_LATER)
	struct timespec ti;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ti);

	return (uint64_t)ti.tv_sec * MICROSEC + (uint64_t)ti.tv_nsec / (NANOSEC / MICROSEC);
#else
	struct task_thread_times_info aTaskInfo;
	mach_msg_type_number_t aTaskInfoCount = TASK_THREAD_TIMES_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t )&aTaskInfo, &aTaskInfoCount)) {
		return 0;
	}

	return (uint64_t)(aTaskInfo.user_time.seconds) + (uint64_t)aTaskInfo.user_time.microseconds;
#endif
}
