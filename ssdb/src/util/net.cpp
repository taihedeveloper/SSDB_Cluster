#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <arpa/inet.h>
#include <errno.h>

#include "net.h"

int get_local_ip(char *ip, int size) {
	int sockfd;
	struct ifconf ifconf;
	struct ifreq ifr[50];
	int ifs;
	int i;
	if (ip == NULL || size <= 0) {
		return -1;
	}

	ip[0] = 0;
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		return -1;
	}

	ifconf.ifc_buf = (char*)ifr;
	ifconf.ifc_len = sizeof(ifr);

	if (ioctl(sockfd, SIOCGIFCONF, &ifconf) == -1) {
		close(sockfd);
		return -1;
	}

	ifs = ifconf.ifc_len / sizeof(ifr[0]);
	for (i = 0; i < ifs; i++) {
		struct sockaddr_in *s_in = (struct sockaddr_in *) &ifr[i].ifr_addr;
		if (!inet_ntop(AF_INET, &s_in->sin_addr, ip, size)) {
			close(sockfd);
			return -1;
		}
		if (strcmp(ip, "127.0.0.1") != 0) {
			break;
		}
	}
	close(sockfd);
	return 0;
}
