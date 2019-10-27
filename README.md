# transfer-logs
Code for transferring rra databases from one toon to another.

Command line utility for transferring data from any toon to another toon. Currently, export.zip, all raw databases and config_happ_pwrusage.xml are supported as data files.
Transfer-logs is written in plain old C, and uses ezxml and junzip for reading xml and zip files, respectively.
ezxml's copyright owner is Aaron Voisine <aaron@voisine.org>, junzip's copyright owner is Joonas Pihlajamaa (firstname.lastname@iki.fi).
Extended copyright statements are available in ezxml.c and junzip.h.
