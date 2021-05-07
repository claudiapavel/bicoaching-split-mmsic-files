from google.cloud import storage
from google.oauth2 import service_account
import json
import io
import os
import logging

def main():

    gcs_client = storage.Client()
    bucket_name = f'claudia_tests'
    destination_bucket = f'claudia_test2'
    bucket = gcs_client.bucket(bucket_name)
    # collect the folders under bucket
    blobs = list(bucket.list_blobs(prefix=None))

    debug = 0
    run_local = 0
    if run_local == 1:
        path_prefix = 'C:/Users/claudia.radoi/Desktop/MMSIC/'
    else:
        path_prefix = '/tmp/'

    doc_line_structure = {"STYPE":[0,1],"TBNAM":[1,31],"NEWBS":[31,33],"DMBTR":[33,49],"WRBTR":[49,65],"MWSKZ":[65,67],"XSKRL":[67,68],"GSBER":[68,72],"KOSTL":[72,82],"VALUT":[82,90],"ZFBDT":[90,98],"ZUONR":[98,116],"SGTXT":[116,166],"WSKTO":[166,182],"ZTERM":[182,186],"ZBD1T":[186,189],"ZBD1P":[189,195],"ZBD2T":[195,198],"ZBD2P":[198,204],"ZBD3T":[204,207],"ZLSPR":[207,208],"REBZG":[208,218],"REBZJ":[218,222],"REBZZ":[222,225],"ZLSCH":[225,226],"MANSP":[226,227],"LZBKZ":[227,230],"LANDL":[230,233],"STCEG":[233,253],"NEWKO":[253,270],"HKONT":[270,280],"XREF1":[280,296],"XREF2":[296,308],"VBUND":[308,314],"XREF3":[314,334],"BEWAR":[334,337],"SKFBT":[337,353],"OP_IX_MGP":[353,368],"ILNLA":[368,381],"MWSSZ":[381,386],"MWSTS":[386,402],"KIND_OF_GOODS":[402,406],"GL_ACC_TYPE":[406,410],"TRANS_TYPE":[410,424],"COND_BLOCK":[424,430],"RGDAT":[430,438],"XLFZB":[438,439],"PRINTDOCNO":[439,449],"VAT_INVNO":[449,465],"REDAT":[465,473],"RETYP":[473,475],"DOCTYP":[475,477],"RENR_25":[477,502],"DATE_ORI_VAT_INV":[502,510],"NUM_ORI_VAT_INV":[510,526],"ORI_RENR":[526,542],"ORI_REDAT":[542,550],"HSNCD":[550,558],"GST_PART":[558,568],"PLC_SUP":[568,571],"FILLER":[571,580],"SENDE":[580,581]}
    doc_tax_structure = {"STYPE":[0,1],"TBNAM":[1,31],"FWSTE":[31,47],"MWSKZ":[47,49],"BSCHL":[49,51],"TXJCD":[51,66],"HWSTE":[66,82],"KSCHL":[82,86],"H2STE":[86,102],"H3STE":[102,118],"FWBAS":[118,134],"HWBAS":[134,150],"H2BAS":[150,166],"H3BAS":[166,182],"TXKRS":[182,192],"CTXKRS":[192,202],"FILLER":[202,232],"SENDE":[232,233]}
    doc_header_structure = {"STYPE":[0,1],"TCODE":[1,21],"BLDAT":[21,29],"BLART":[29,31],"BUKRS":[31,35],"BUDAT":[35,43],"MONAT":[43,45],"WAERS":[45,50],"KURSF":[50,60],"BELNR":[60,70],"WWERT":[70,78],"XBLNR":[78,94],"BVORG":[94,110],"BKTXT":[110,135],"PARGB":[135,139],"AUGLV":[139,147],"VBUND":[147,153],"XMWST":[153,154],"DOCID":[154,164],"BARCD":[164,204],"STODT":[204,212],"BRNCH":[212,216],"NUMPG":[216,219],"STGRD":[219,221],"KURSF_M":[221,231],"AUGTX":[231,281],"XPRFG":[281,282],"XBWAE":[282,283],"PRINTDOCNO":[283,293],"WEDAT":[293,301],"ABDAT":[301,309],"FIDAT":[309,317],"VORGN":[317,331],"SENDE":[331,332]}
    split_files = ["FB01", "ZBSEG", "BTAX"]

    def mv_blob(source_bucket_name, source_blob_name, dest_bucket_name, dest_blob_name, del_ind=0):
        """"
        Function for moving files between directories or buckets. it will use GCP's copy function then delete the blob from the old location.
        source_bucket_name: name of bucket
        source_blob_name: str, name of file (ex. 'data/some_location/file_name')
        destination_bucket: name of bucket (can be same as original if we're just moving around directories)
        dest_blob_name: str, name of file in new directory in target bucket (ex. 'data/destination/file_name')
        """
        source_bucket = gcs_client.get_bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        destination_bucket = gcs_client.get_bucket(dest_bucket_name)

        # copy to new destination
        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, dest_blob_name)
        print(f'File moved from {source_blob} to {new_blob}')

        # delete the original files: 0 - no deletion; 1 = delete only files; 2 = delete the files & the containing folder
        if del_ind == 1:
            source_blob.delete()
        elif del_ind == 2:
            source_blob.delete()
            print(f'File removed from {source_blob}')
            source_blob = source_bucket.blob(f"{removeAfterN(source_blob.name, 2, '/', 1)}/")
            source_blob.delete()
            print(f'File removed from {source_blob}')

    # function to remove string after nth occurence of a delimiter:
    # yourStr = your string; nth = number of occurences; occurenceOf = delimiter to check;
    # after_ind: 1 if the string on the right the delimiter is found should be removed; 0 if the string on the left of the delimiter should be removed

    def removeAfterN(yourStr, nth, occurenceOf, after_ind):
        if after_ind == 1:
            return occurenceOf.join(yourStr.split(occurenceOf)[:nth])
        if after_ind == 0:
            return occurenceOf.join(yourStr.split(occurenceOf)[nth:])

    def replace_line(col, file_structure):
        replaced_line = line
        for i in col:
            comma = ','
            dot = '.'
            start = int(file_structure[i][0])
            end = int(file_structure[i][1])
            if debug == 1:
                print('start =', start)
                print('end = ', end)
                print('')
            if comma in line[start:end] or dot in line[start:end]:
                initial_len = len(line[start:end])
                if debug == 1:
                    print('initial_len = ', initial_len)
                replaced_col = line[start:end].replace('.', '').replace(',', '.').strip()
                if debug == 1:
                    print('replaced_col = ', replaced_col)
                replaced_len = len(replaced_col)
                if debug == 1:
                    print('replaced_len = ', replaced_len)
                if line[start:end][-1] == ' ':
                    replaced_line = replaced_line[0:start] + (
                                initial_len - replaced_len - 1) * ' ' + replaced_col + ' ' + replaced_line[end:]
                else:
                    replaced_line = replaced_line[0:start] + (
                                initial_len - replaced_len) * ' ' + replaced_col + replaced_line[end:]
                if debug == 1:
                    print('original_line = ', line)
                    print('replaced_line = ', replaced_line)
            else:
                continue
        return (replaced_line)

    number_lines_file = open(f"{path_prefix}log.txt", 'w')

    # for files in bucket.list_blobs(prefix=None).pages:
    #     for file in files:
    for file in blobs:
        if file.name.find('/archived/') != -1 or file.name.find('.dat') == -1:
            continue
        else:
            folder_name = f"{removeAfterN(file.name, 2, '/', 1)}/"
            archived_folder = f"{removeAfterN(file.name, 1, '/', 1)}/archived/"
            timestamp_folder = f"{removeAfterN(removeAfterN(file.name, 1, '/', 0), 1, '/', 1)}/"
            current_file_name = f"{removeAfterN(file.name, 2, '/', 0)}"
            # file.name[-20:]

            print('folder_name = ', folder_name)
            print('archived_folder = ', archived_folder)
            print('timestamp_folder = ', timestamp_folder)
            print('current_file_name = ', current_file_name)

            if not os.path.exists(f"{path_prefix}{folder_name}"):
                os.makedirs(f"{path_prefix}{folder_name}")

            # open new files for each file with same structure
            open_header = open(f"{path_prefix}{folder_name}FB01.{current_file_name}", 'w')
            open_line = open(f"{path_prefix}{folder_name}ZBSEG.{current_file_name}", 'w')
            open_tax = open(f"{path_prefix}{folder_name}BTAX.{current_file_name}", 'w')
            open_nosplit = open(f"{path_prefix}{folder_name}nosplit.{current_file_name}", 'w')

            if debug == 1:
                open_header_before = open(f"{path_prefix}FB01_beforereplace.{current_file_name}", 'w')
                open_line_before = open(f"{path_prefix}ZBSEG_beforereplace.{current_file_name}", 'w')
                open_tax_before = open(f"{path_prefix}BTAX_beforereplace.{current_file_name}", 'w')

            blob = bucket.get_blob(file.name)
            downloaded_blob = blob.download_as_string()
            bytes_buffer = io.BytesIO(downloaded_blob)
            wrapped_text = io.TextIOWrapper(bytes_buffer)

            key = 0
            cnt_total = 0
            cnt_lines_docheader = 0
            cnt_lines_docline = 0
            cnt_lines_doctax = 0
            cnt_lines_nosplit = 0

            for line in wrapped_text:

                cnt_total += 1

                if line.startswith("1FB01"):
                    key += 1

                    if debug == 1:
                        open_header_before.writelines(f"{line.rstrip()}{key}-{current_file_name} \n")

                    replaced_line = replace_line(["KURSF", "KURSF_M"], doc_header_structure)
                    file_name = (str(key) + "-" + (current_file_name).ljust(50, " "))[:50]
                    open_header.writelines(f"{replaced_line.rstrip()}{file_name} \n")
                    cnt_lines_docheader += 1
                    # secondary key value is asigned only if a previous FB01 existed
                    sec_key = 0

                elif line.startswith("2ZBSEG"):
                    sec_key += 1

                    if debug == 1:
                        open_line_before.writelines(f"{line.rstrip()}{key}-{current_file_name} \n")

                    replaced_line = replace_line(
                        ["DMBTR", "WRBTR", "WSKTO", "ZBD1P", "ZBD2P", "SKFBT", "MWSSZ", "MWSTS"], doc_line_structure)
                    file_name = (str(key) + "-" + (current_file_name).ljust(50, " "))[:50]
                    open_line.writelines(f"{replaced_line.rstrip()}{file_name} \n")
                    cnt_lines_docline += 1

                elif (line.startswith("2BBTAX") or line.startswith("2ZBTAX")):
                    sec_key += 1

                    if debug == 1:
                        open_tax_before.writelines(f"{line.rstrip()}{key}-{current_file_name} \n")

                    replaced_line = replace_line(
                        ["FWSTE", "HWSTE", "H2STE", "H3STE", "FWBAS", "HWBAS", "H2BAS", "H3BAS", "TXKRS", "CTXKRS"],
                        doc_tax_structure)
                    file_name = (str(key) + "-" + (current_file_name).ljust(50, " "))[:50]
                    open_tax.writelines(f"{replaced_line.rstrip()}{file_name} \n")
                    cnt_lines_doctax += 1
                else:
                    open_nosplit.writelines(f"{line}")
                    cnt_lines_nosplit += 1

            open_header.close()
            open_line.close()
            open_tax.close()
            open_nosplit.close()

            if debug == 1:
                open_header_before.close()
                open_line_before.close()
                open_tax_before.close()

            number_lines_file.writelines(
                f"\n File {file.name}: \n Total number of rows = {cnt_total}, \n Doc header number of lines = {cnt_lines_docheader}, \n Doc line number of lines = {cnt_lines_docline}, \n Doc tax number of files = {cnt_lines_doctax}, \n Number of lines not split = {cnt_lines_nosplit}")

            # check if number of lines match
            if cnt_total == cnt_lines_docheader + cnt_lines_docline + cnt_lines_doctax + cnt_lines_nosplit:
                number_lines_file.writelines("\n CORRECT: total number of lines matches after split")
                try:
                    # for each split file upload it into bucket
                    for i in split_files:
                        # upload files from local/tmp to the bucket folder
                        logging.info(f"{folder_name}{i}.{current_file_name}")
                        logging.info(f"{path_prefix}{folder_name}{i}.{current_file_name}")
                        blob = bucket.blob(f"{folder_name}{i}.{current_file_name}")
                        blob.upload_from_filename(f"{path_prefix}{folder_name}{i}.{current_file_name}")
                        # transfer files to another bucket
                        mv_blob(bucket_name, blob.name, destination_bucket, blob.name, 0)
                        # archive files after transfer
                        mv_blob(bucket_name, blob.name, bucket_name, f"{archived_folder}{timestamp_folder}{i}.{current_file_name}", 1)
                except:
                    print('Exception occured')
                else:
                    mv_blob(bucket_name, f"{folder_name}{current_file_name}", bucket_name, f"{archived_folder}{timestamp_folder}{current_file_name}", 2)
            else:
                number_lines_file.writelines("\n ERROR: total number of lines does not match after split")

    number_lines_file.close()


main()