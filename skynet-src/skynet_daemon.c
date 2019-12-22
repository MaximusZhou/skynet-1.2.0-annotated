#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <signal.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>

#include "skynet_daemon.h"

static int
check_pid(const char *pidfile) {
	int pid = 0;
	FILE *f = fopen(pidfile,"r");
	if (f == NULL)
		return 0;
	int n = fscanf(f,"%d", &pid);
	fclose(f);

	if (n !=1 || pid == 0 || pid == getpid()) {
		return 0;
	}

	// 使用kill函数以及第二参数0，检测进程pid是否存在
	// ESRCH 错误码表示没这个进程
	if (kill(pid, 0) && errno == ESRCH)
		return 0;

	// 表示保存在文件的中pid进程已经存在了，skynet已经在运行了
	return pid;
}

static int
write_pid(const char *pidfile) {
	FILE *f;
	int pid = 0;
	int fd = open(pidfile, O_RDWR|O_CREAT, 0644);
	if (fd == -1) {
		fprintf(stderr, "Can't create pidfile [%s].\n", pidfile);
		return 0;
	}

	// fdopen打开指定的文件描述符
	f = fdopen(fd, "r+");
	if (f == NULL) {
		fprintf(stderr, "Can't open pidfile [%s].\n", pidfile);
		return 0;
	}

	// LOCK_EX 表示该进程对指定的文件加上exclusive lock
	// LOCK_NB 表示加锁是非阻塞的，默认flock会阻塞的
	if (flock(fd, LOCK_EX|LOCK_NB) == -1) {
		int n = fscanf(f, "%d", &pid);
		fclose(f);
		if (n != 1) {
			fprintf(stderr, "Can't lock and read pidfile.\n");
		} else {
			fprintf(stderr, "Can't lock pidfile, lock is held by pid %d.\n", pid);
		}
		return 0;
	}

	pid = getpid();
	if (!fprintf(f,"%d\n", pid)) {
		fprintf(stderr, "Can't write pid.\n");
		close(fd);
		return 0;
	}
	fflush(f);

	return pid;
}

static int
redirect_fds() {
	int nfd = open("/dev/null", O_RDWR);
	if (nfd == -1) {
		perror("Unable to open /dev/null: ");
		return -1;
	}
	if (dup2(nfd, 0) < 0) {
		perror("Unable to dup2 stdin(0): ");
		return -1;
	}
	if (dup2(nfd, 1) < 0) {
		perror("Unable to dup2 stdout(1): ");
		return -1;
	}
	if (dup2(nfd, 2) < 0) {
		perror("Unable to dup2 stderr(2): ");
		return -1;
	}

	close(nfd);

	return 0;
}

int
daemon_init(const char *pidfile) {
	// 从保存进程id的文件中，读取相应的进程pid，检测相应的pid是否存在，如果存在，则返回相应的pid
	int pid = check_pid(pidfile);

	if (pid) {
		fprintf(stderr, "Skynet is already running, pid = %d.\n", pid);
		return 1;
	}

#ifdef __APPLE__
	fprintf(stderr, "'daemon' is deprecated: first deprecated in OS X 10.5 , use launchd instead.\n");
#else
	// 该系统接口就把当前进程从控制台进程中detach，
	// 摆脱控制台的控制，并作为系统的后台进程来运行
	if (daemon(1,1)) {
		fprintf(stderr, "Can't daemonize.\n");
		return 1;
	}
#endif

	// 把当前进程的 pid 写入到pidfile文件中
	pid = write_pid(pidfile);
	if (pid == 0) {
		return 1;
	}

	// 把fd 0 1 2 ，即标准输入、输出和错误输出重定向到/dev/null
	if (redirect_fds()) {
		return 1;
	}

	return 0;
}

int
daemon_exit(const char *pidfile) {
	// 删除保存进程pid的文件
	return unlink(pidfile);
}
