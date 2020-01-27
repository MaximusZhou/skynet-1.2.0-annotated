#include "skynet.h"

#include "skynet_monitor.h"
#include "skynet_server.h"
#include "skynet.h"
#include "atomic.h"

#include <stdlib.h>
#include <string.h>

// 每一个worker线程，对应一个这样的结构体实例
struct skynet_monitor {
	int version; // 这个值一直累加，在消息处理前后都会累加一次
	int check_version;  // 最近一次调用检测的版本号
	uint32_t source; // 要处理的消息的发送的服务的handle
	uint32_t destination; // 处理消息服务对应的handle
};

// 线程启动的时候调用的，为每一个工作线程调用一次，分配一个相应的结构体
struct skynet_monitor * 
skynet_monitor_new() {
	struct skynet_monitor * ret = skynet_malloc(sizeof(*ret));
	memset(ret, 0, sizeof(*ret));
	return ret;
}

// 系统关闭的时候调用
void 
skynet_monitor_delete(struct skynet_monitor *sm) {
	skynet_free(sm);
}

// 这个接口在执行消息的回调的函数前后调用
// 执行回调函数前调用，设置消息的发送和处理服务的handle，执行完成后，清空这个值
// 每调用一次增加version的值
void 
skynet_monitor_trigger(struct skynet_monitor *sm, uint32_t source, uint32_t destination) {
	sm->source = source;
	sm->destination = destination;
	ATOM_INC(&sm->version);
}

// 在monitor线程函数中轮询调用，每隔5秒调用一次
// 如果两次调用两次间隔的版本号没变，则说明这两个间隔之间，一直在处理某条消息，执行某个服务的回调函数
// 当前检测到这种情况，则调用 skynet_context_endless，并且给出报警
void 
skynet_monitor_check(struct skynet_monitor *sm) {
	if (sm->version == sm->check_version) {
		if (sm->destination) {
			skynet_context_endless(sm->destination);
			skynet_error(NULL, "A message from [ :%08x ] to [ :%08x ] maybe in an endless loop (version = %d)", sm->source , sm->destination, sm->version);
		}
	} else {
		sm->check_version = sm->version;
	}
}
