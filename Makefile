# Specify if network connection should be made using Tunslip (1) or a Gateway (0)
USE_TUNSLIP6=1

# save some memory by disabling web interface
USE_WEBSERVER=0

CONTIKI_TARGET_SOURCEFILES += dht22.c tsl256x.c


all: mqtt_publisher

DEFINES+=PROJECT_CONF_H=\"project-conf.h\"

CONTIKI_WITH_IPV6 = 1

# adjust accordingly to your folder location
CONTIKI = ../../contiki

APPS += mqtt

# Linker size optimization
SMALL=1

ifeq ($(USE_TUNSLIP6), 1)

DEFINES+=USE_TUNSLIP6=1

DEFINES+=UIP_FALLBACK_INTERFACE=rpl_interface
DEFINES+=UUIP_CONF_TCP=1

PROJECT_SOURCEFILES += slip-bridge.c
ifeq ($(USE_WEBSERVER), 1)
DEFINES+=WEBSERVER=1
PROJECT_SOURCEFILES += httpd-simple.c
endif
	
ifeq ($(PREFIX),)
PREFIX = fd00::1/64
endif
endif

include $(CONTIKI)/Makefile.include

connect-router:	$(CONTIKI)/tools/tunslip6
	sudo $(CONTIKI)/tools/tunslip6 $(PREFIX)

connect-router-cooja:	$(CONTIKI)/tools/tunslip6
	sudo $(CONTIKI)/tools/tunslip6 -a 127.0.0.1 $(PREFIX)


