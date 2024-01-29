#!/bin/bash


###################################################################################################
# ${var%Pattern} Remove from $var the shortest part of $Pattern that matches the back end of $var. 
# ${var#Pattern} Remove from $var the shortest part of $Pattern that matches the front end of $var.
###################################################################################################
# 2022/06/15 
# I feel that I can do better, but time, resources, and energy are limited.
# 
# Seek first to understand, then to be understood.
#

#C:\Users\jitin\AppData\Roaming\QGIS\QGIS3\profiles\default\python\plugins\ROSA_release\Lily\bin
#/cygdrive/c/Users/jitin/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/ROSA_release/Lily/bin
#
#
bak_destination='raid_d'

#if [[ $(date +%u) -gt 4 ]]; 

#   then  bak_destination='Artemis5' ; 
#fi

bak_destination='Artemis5' ; 
#
#
homedir="/cygdrive/g"

respository="laluna@192.192.138.115:/mnt/autumn/backup_ASUS-TS700E9"
history_dir="backup_TS700E9_history"

#
#items list for raid_list
raid_list=(\
      		"${homedir}/zoo.*","${respository}/crying_freeman"\
      		"${homedir}/pylily_family","${respository}/crying_freeman"\
      		"${homedir}/data_[new]*","${respository}/crying_freeman"\
      		"${homedir}/NCREE_GIS","${respository}/crying_freeman"\
)

#
#
#options for /usr/bin/rsync to raid6
raid_argu="-avz --delete --exclude '.*' --backup --backup-dir=${history_dir}"

#
#
#
	for b in ${raid_list[@]} ;
		do
			src=${b%,*};
			des=${b#*,};
			/usr/bin/echo -e "rsync $raid6_argu";
			/usr/bin/echo -e "rsync ${src} TO ${des}";
			/usr/bin/rsync $raid_argu ${src} ${des};
			/usr/bin/echo -e "......End\n";
		done
#
