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

#
#
#
bak_destination='raid_d'

#if [[ $(date +%u) -gt 4 ]]; 

#   then  bak_destination='Artemis5' ; 
#fi

bak_destination='Artemis5' ; 
#
#
homedir="/cygdrive/c/Users/jitin/Desktop"
respository="${homedir}/backup_disk/backup_ASUS-TS700E9"
history_dir="${homedir}/backup_disk/backup_ASUS-TS700E9_history"

#
#
#items list for raid_list
raid_list=(\
      		"${homedir}/crying_freeman/zoo.*","${respository}/crying_freeman"\
      		"${homedir}/crying_freeman/pylily_family","${respository}/crying_freeman"\
			"${homedir}/crying_freeman/ctao_data","${respository}/crying_freeman"\
      		"${homedir}/crying_freeman/data_[nw]*","${respository}/crying_freeman"\
      		"${homedir}/crying_freeman/NCREE_GIS","${respository}/crying_freeman"\
      		"${homedir}/crying_freeman/OneDrive","${respository}/crying_freeman"\
)

#
#
#options for /usr/bin/rsync to raid6
raid_argu="-avz --delete --exclude '.*' --backup --backup-dir=${history_dir}"

#
#
#
case $bak_destination in
	"raid_d")
		for b in ${raid_list[@]} ;
			do
				src=${b%,*};
				des=${b#*,};
				/usr/bin/echo -e "rsync $raid6_argu";
				/usr/bin/echo -e "rsync ${src} TO ${des}";
				/usr/bin/rsync $raid_argu ${src} ${des};
				/usr/bin/echo -e "......End\n";
			done
	;;

	"Artemis5")  
		/usr/bin/echo -e 'connect to  Artemis5 ......';
		#segm 1
		remote_host="laluna@192.192.138.115:";
		remote_respository="laluna@192.192.138.115:/mnt/autumn";
		remote_history_dir="/mnt/autumn/backup_ASUS-TS700E9_history";
		operation="-avz --delete --backup --backup-dir=${remote_history_dir}";
		#segm 2
		/usr/bin/echo -e "rsync ${operation} ${respository} ${remote_respository}";
		/usr/bin/rsync ${operation} ${respository} ${remote_respository};
	;;
	*) 	
	#segm
		/usr/bin/echo -e 'backup_to raid_d or Artemis5';
	;;
esac
#
