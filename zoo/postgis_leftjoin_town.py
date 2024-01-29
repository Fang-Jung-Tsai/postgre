import postgis_CE13058

if __name__ == '__main__':
    
    try:
        postgis = postgis_CE13058.postgis_CE13058()

        arg_town = postgis.read_geometry( 'argu_town' )

        #drop all columns except 'town_code', 'town_name' and 'geometry' of arg_twon
        arg_town = arg_town[['town_code', 'town_name', 'geom']]


        cwa_d0047 = postgis.read_data( 'data_rosa_ctyang_cwa_d0047' )

        # arg_twon left join cwa_d0047 on arg_town.town_name == cwa_d0047.locationName
        arg_town = arg_town.merge(cwa_d0047, how='left', left_on='town_name', right_on='locationName')

        postgis.write_geometry(arg_town, 'data_rosa_ctyang_town')   

      
    except Exception as e:

        print(e)
        raise e