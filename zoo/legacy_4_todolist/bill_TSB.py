import PyPDF2
import pandas as pd
import re

def parse_pdf(path, password):
    # 開啟PDF檔案
    with open(path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        # 解密
        reader.decrypt(password)

        content =  ""   
        # 提取PDF內容並寫入到TXT檔案
        with open("output.txt", "w") as output:
            for page in reader.pages:
                text = page.extract_text()
                content += text
                output.write(text)

    return content

def parse_TSB(content):
    #find start of the keyword "下列消費明細"
    #and find the end of the keyword "您的"
    #then get the string between them
    start = content.find("下列消費明細")
    end = content[start:].find("您的")
    end = start + end
    content = content[start:end]

    # split content to lines by "\n"
    lines = content.strip().split("\n")

    # classify the lines to 4 types
    #   re pattern include alphanum and symbol , so use \S instead of \w
    #   it also include the space, so use \s+ to match the space
    # 1. card info
    pattern1 = r"\s+(\S+)\s+(\S+)\s+\(卡號末四碼:(\d{4})\)"
    # 2. transaction info
    pattern2 = r"(\d{3}/\d{2}/\d{2})\s+(\d{3}/\d{2}/\d{2})"
    # 3. transaction info continue
    pattern3 = r"([\S\s]+)"
    # 4. other info
    
    # create a list of list to store the lines and type
    lines_type = []
    for key in lines:
        if re.search(pattern1, key) and re.search(pattern2, key) :
            lines_type.append([key, 5])

        elif re.search(pattern1, key):
            lines_type.append([key, 1])

        elif re.search(pattern2, key):
            lines_type.append([key, 2])

        elif re.search(pattern3, key): 
            lines_type.append([key, 3]) 

        else:
            lines_type.append([key, 4])

    # create a list of list, divide the type 5 into two lines 
    lines_type2 = []
    for i in range(len(lines_type)):
        if lines_type[i][1] == 5:

            text = lines_type[i][0]
            #split the text into two parts by long space
            text = re.split(r"\s{3,}", text)
            lines_type2.append([text[0], 2])
            lines_type2.append([text[1], 1])
        else:
            lines_type2.append(lines_type[i])   

    #check the lines_type if type 3 follow type 2 , 
    #then add type 3 after type 2
    for i in range(len(lines_type2)-1):
        if lines_type2[i][1] == 2 and lines_type2[i+1][1] == 3:
            lines_type2[i][0]= lines_type2[i][0] + ' ' + lines_type2[i+1][0]
            lines_type2[i+1][1] = 6

    lines_type = [x for x in lines_type2 if x[1] != 6]

    # check and divide the text to attributes of transcations by re pattern
    # data format (transaction date, post date, transaction detail, transaction amount)
    # transaction amount is numbers with ','  (regular expression pattern  \d+(?:,\d+)*  )
    creditcard_pattern  = r"\s*(\S+)\s+(\S+)\s+\(卡號末四碼:(\d{4})\)"
    transaction_pattern = r"(\d{3}/\d{2}/\d{2})\s+(\d{3}/\d{2}/\d{2})\s*([\S\s]{8,})\s*(-?\d+(?:[,\d]*))"

    card_info = {}
    result_list = []
    for line in lines_type:
        text = line[0]
        type = line[1]
        if type == 1:
            match = re.search(creditcard_pattern, text)
            if match:
                card_info['name'] = match.group(1)
                card_info['card_no'] = match.group(3)
                card_info['card_holder'] = match.group(2)
                print(card_info)

        if type == 2:
            match = re.search(transaction_pattern, text)
            if match:
                target = match.group(0)
                m2 = re.search(r"-?\d+(?:,\d+)*$",  target )                
                if m2:
                    date = match.group(1)
                    post_date = match.group(2)
                    amount = m2.group(0)

                    # remove the transaction amount from target                    
                    target = target[:-len(m2.group(0))]            
                    # remove the date and the post date from target
                    detail = target[len(date)+len(post_date)+1:]
                    
                    # merge the result to dictionary
                    result = {'data': date, 'post_date': post_date, 'detail': detail, 'amount': amount}
                    result.update(card_info) 
                    result_list.append(result)  

    return pd.DataFrame(result_list)

if __name__ =='__main__':
    from user_argument import user_argument

    user = user_argument()

    content = parse_pdf(r"c:\users\jitin\Downloads\TSB_202305.pdf", user.ID)
    dataframe = parse_TSB(content)

    print(dataframe)