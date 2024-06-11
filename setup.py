import sqlite3
import shutil
import os
# ssid in kismet file (devices:device: dot11.advertisedssid.ssid or dot11.probedssid.ssid (hidden ssid))
# delete where strongest_signal = 0
# delete where ssid = "eduroam", "HZI_Guest", "HZI_Member"
# if a ssid appear several times, select the devmac and the location(avg_lat + avg_lon) per ssid
# for every ssid search the devmac with the strongest signal 
# ignore 24:1F:BD:00:00:00 - 24:1F:BD:FF:FF:FF (MA-L)   2E:D1:C6 VW    

# rssi: https://www.researchgate.net/figure/FM-RSSI-level-conversion_tbl1_220535505


# Using SQL to extract the valid information from the database
def parse_kismet_log(kismet_log_file):
    copied_file = os.path.join(os.path.dirname(kismet_log_file), "Copy_" + os.path.basename(kismet_log_file))
    shutil.copyfile(kismet_log_file, copied_file)
    
    conn = sqlite3.connect(copied_file)
    cursor = conn.cursor()

    # Delete information that won't be used
    cursor.execute("DELETE FROM devices WHERE strongest_signal = 0")
    cursor.execute("""
    DELETE FROM devices
    WHERE device LIKE '%eduroam%'
       OR device LIKE '%HZI_Guest%'
       OR device LIKE '%HZI_Member%'
    """)

    # Delete irrelevant MAC Address
    cursor.execute("""
    DELETE FROM devices
    WHERE devmac LIKE '24:1F:BD:%'
       OR devmac LIKE '2E:D1:C6:%' 
       OR devmac LIKE '8C:1D:96:%'
       OR devmac LIKE '64:D6:9A:%'
    """
    )

    # Add column "ssid"
    cursor.execute("""
    ALTER TABLE devices
    ADD COLUMN ssid TEXT
    """)

    # Extract ssid from the column "device"
    # ssid Field is NULL
    cursor.execute("""
    UPDATE devices
    SET ssid = 
        CASE 
            WHEN json_extract(device, '$."dot11.device"."dot11.device.last_beaconed_ssid_record"."dot11.advertisedssid.ssid"') IS NOT NULL 
            THEN json_extract(device, '$."dot11.device"."dot11.device.last_beaconed_ssid_record"."dot11.advertisedssid.ssid"')
            ELSE json_extract(device, '$."dot11.device"."dot11.device.last_probed_ssid_record"."dot11.probedssid.ssid"')
        END;
    """)

    conn.commit()
    conn.close()
    
    return copied_file


# Find the mac address and location of repeated ssid
def select_repeated_ssids(kismet_log_file):
    conn = sqlite3.connect(kismet_log_file)
    cursor = conn.cursor()

    # Select SSIDs that appear more than once
    # cursor.execute("""
    # SELECT ssid
    # FROM devices
    # GROUP BY ssid
    # HAVING COUNT(*) > 1
    # """)
    # repeated_ssids = [row[0] for row in cursor.fetchall()]

    # Create a new table to store the results
    cursor.execute("""
    CREATE TABLE rogue_wlan AS
    SELECT *
    FROM devices
    WHERE (ssid, strongest_signal) IN (
        SELECT ssid, MIN(strongest_signal)
        FROM devices
        GROUP BY ssid
    )
    """)
    #.format(','.join(['?']*len(repeated_ssids))), repeated_ssids

    conn.commit()
    conn.close()

    return kismet_log_file


def main():
    kismet_log_folder = "kismet_logs"
    result_folder = "results"

    for kismet_log_file in os.listdir(kismet_log_folder):
        kismet_log_file = "kismet_logs/" + kismet_log_file
        access_points = parse_kismet_log(kismet_log_file)
        rogue_wifi = select_repeated_ssids(access_points)

        file_name = "result_" + os.path.basename(rogue_wifi) 
        output_file = f'{result_folder}/{file_name}'
        os.rename(rogue_wifi, output_file)


if __name__ == "__main__":
    main()
