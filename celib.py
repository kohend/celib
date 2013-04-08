#!/usr/bin/env python

import simplejson as json
import httplib2
import sys
import mmap
import re
import os

class CE_files():
    def __init__(self,endpoint=None,chunk_size=5242880):
        self.http=httplib2.Http()
        if endpoint:
            if not re.match('^http(s)*://.*',endpoint):
                raise RuntimeError()
            self.endpoint="%s/json" % endpoint
        else:
            self.endpoint="https://service.pogoplug.com/svc/api/json"
        if chunk_size>1:
            self.chunk_size=chunk_size
        else:
            raise RuntimeError()

    def __validate_token(self):
        login_resp=self.get_request("%s/loginUser?email=%s&password=%s"%(self.endpoint,self.user,self.password),addToken=False)
        if login_resp:
            self.valtoken=CE_files.json_parse(login_resp,'valtoken')
        else:
            raise RuntimeError('Could not login')

    def connect_token(self,valtoken):
        self.valtoken=valtoken
        device_list=self.get_request("%s/listDevices?valtoken=%%s"%(self.endpoint))
        if device_list:
            devices_parsed=CE_files.json_parse(device_list,'devices')            
            services_list=[]
            for device in devices_parsed:
                services_list+=device['services']
            self.__set_service(services_list)

    def connect(self,user,password):
        self.user=user
        self.password=password
        self.__validate_token()
        device_list=self.get_request("%s/listDevices?valtoken=%%s"%(self.endpoint))
        if device_list:
            devices_parsed=CE_files.json_parse(device_list,'devices')            
            services_list=[]
            for device in devices_parsed:
                services_list+=device['services']
            self.__set_service(services_list)


    def listFiles(self,parentId=None):
        if parentId:
            query="&parentid=%s" % parentId
        else:
            query=""
        output=self.get_request("%s/listFiles?valtoken=%%s&deviceid=%s&serviceid=%s%s"%(self.endpoint,self.deviceid,self.serviceid,query))
        output_parsed=CE_files.json_parse(output,'files')
        files=dict()
        for file_info in output_parsed:
            files[file_info['name']]=file_info
        offset=1
        while len(output_parsed) > 0:
            output=self.get_request("%s/listFiles?valtoken=%%s&deviceid=%s&serviceid=%s&pageoffset=%d%s"%(self.endpoint,self.deviceid,self.serviceid,offset,query))
            output_parsed=CE_files.json_parse(output,'files')
            for file_info in output_parsed:
                files[file_info['name']]=file_info
            offset+=1
        return files
                   
    def createFile(self,name,parentid=None,path=None,type=0):
        parent=None
        if parentid:
            parent=parentid
        else:
            if path:
                parent=self._get_file_from_list(path)['fileid']
        if parent and len(parent) > 0:
            query="&parentid=%s" % parent
        else:
            query=""
        output=self.get_request("%s/createFile?valtoken=%%s&deviceid=%s&serviceid=%s&filename=%s&type=%d%s"%(self.endpoint,self.deviceid,self.serviceid,name,type,query),returnError=True)
        output_parsed=CE_files.json_parse(output,'file')
        return output_parsed

    def removeFile(self,fileid=None):
        output=self.get_request("%s/removeFile?valtoken=%%s&deviceid=%s&serviceid=%s&fileid=%s"%(self.endpoint,self.deviceid,self.serviceid,fileid),returnError=False)
        return output!=None

    def _clean_path(self,path):
        split_path=path.split(os.sep)
        index=0
        while index < len(split_path):
            if len(split_path[index])==0 or split_path[index]=='.':
                del split_path[index]
            else:
                index+=1
        return split_path


    def create_path(self,path):
        parent=None
        parentid=0
        file_list=self.listFiles(parentid)
        split_path=self._clean_path(path)
        index=0
        for part in split_path:
            if part in file_list:
                if int(file_list[part]['type']) == 1:
                    parent=file_list[part]
                    parentid=file_list[part]['fileid']
            else:
                parent=self.createFile(part,parentid=parentid,type=1)
                parentid=parent['fileid']
            file_list=self.listFiles(parentid)
            index+=1
        if parent:
            return parent
                

    def _get_file_from_list(self,path):
        parent=None
        parentid=None
        file_list=self.listFiles(parentid)
        split_path=self._clean_path(path)
        index=0
        for part in split_path:
            if part in file_list:
                if int(file_list[part]['type']) == 1 and index < len(split_path)-1:
                    parent=file_list[part]
                    parentid=file_list[part]['fileid']
                elif index >= len(split_path)-1:
                    return file_list[part]
                else:
                    return None
            else:
                return None
            file_list=self.listFiles(parentid)
            index+=1
                

    def getFile(self,path=None,fileid=None):
        if fileid and len(fileid) > 0:
            query="&fileid=%s" % fileid
        else:
            if path and len(path) > 0:
                return self._get_file_from_list(path)
                query="&path=%s" % path
            else:
                query=""
        output=self.get_request("%s/getFile?valtoken=%%s&deviceid=%s&serviceid=%s%s"%(self.endpoint,self.deviceid,self.serviceid,query))
        output_parsed=CE_files.json_parse(output,'file')
        return output_parsed
        

    def __set_service(self,service_list):
        for service in service_list:
            if service['type']=='xce:plugfs:cloud' and int(service['online'])==1:
                self.deviceid=service['deviceid']
                self.serviceid=service['serviceid']
                if 'apiurl' in service and len(service['apiurl'])>0:
                    self.endpoint="%sjson"%service['apiurl']
                return True
        return False
                
    def put_file(self,filename,fileid,name=''):
        url=re.sub("/api/json","/files/%%s/%s/%s/%s/%s" % (self.deviceid,self.serviceid,fileid,name),self.endpoint)
        url=re.sub("^https:","http:",url)
        file_to_put=open(filename,'rb')
        
#        raise RuntimeError("filename:%s,fileid:%s,name:%s,url:%s" %(filename,fileid,name,url))
        sent=0
        buff=file_to_put.read(self.chunk_size)
        
        while len(buff)>0:
            print "range:%s" % ("bytes=%d-%d"%(sent,sent+len(buff)-1))
            (resp,content)=self.http.request(url%self.valtoken,method='PUT',body=buff,headers={'range':"bytes=%d-%d"%(sent,sent+len(buff)-1)})
            if resp.status==500 and content and content.find('ecode')!=-1 and int(CE_files.json_parse(content,'HB-EXCEPTION')['ecode'])==606:
                self.__validate_token()
#                (resp,content)=self.http.request(url%self.valtoken,method='PUT',body=mmap.mmap(file_to_put.fileno(),0,access=mmap.ACCESS_READ))
            elif resp.status!=200 and resp.status!=206:
                raise RuntimeError("headers:%s,resp:%s,filename:%s,fileid:%s,name:%s,content:%s,url:%s" %("range:bytes=%d-%d"%(sent,sent+len(buff)-1),resp,filename,fileid,name,content,url))
            else:
                sent+=len(buff)
                buff=file_to_put.read(self.chunk_size)
                

#        (resp,content)=self.http.request(url%self.valtoken,method='PUT',body=mmap.mmap(file_to_put.fileno(),0,access=mmap.ACCESS_READ))
        file_to_put.close()
        return resp.status==200

    def retrieve_file(self,fileid,filename): 
        url=re.sub("/api/json","/files/%%s/%s/%s/%s/stream" % (self.deviceid,self.serviceid,fileid),self.endpoint)
        #url=re.sub("^https:","http:",url)
        file_to_get=open(filename,'wb')
        meta=self.getFile(fileid=fileid)
        recv=0
        while recv <= long(meta['size']):
            to_get=recv+self.chunk_size-1
            if to_get > long(meta['size']):
                to_get=long(meta['size'])
            (resp,content)=self.http.request(url%self.valtoken,method='GET',headers={'range':"bytes=%d-%d"%(recv,to_get)})
            if resp.status==200 or resp.status==206:
                file_to_get.write(content)
                print "response:%s\n range:%s" % (resp,"bytes=%d-%d"%(recv,to_get))
                recv=to_get+1
            elif resp.status==500 and content and content.find('606')!=-1:
                self.__validate_token()
            else:
                print "response:%s\n range:%s" % (resp,"bytes=%d-%d"%(recv,to_get))
                raise RuntimeError("response:%s\n range:%s, url: %s" % (resp,"bytes=%d-%d"%(recv,to_get),url))
        file_to_get.close()
        return resp.status==200

        return None
            
    @staticmethod
    def json_parse(string,token=None):
        parsed=json.loads(string)
        if token and len(token)>0 and token in parsed:
            return parsed[token]
        elif token and len(token)>0:
            return []
        else:
            return parsed
            

    def get_request(self,url,returnError=False,count=0,addToken=True):
        if addToken:
            (resp,content)=self.http.request(url%self.valtoken)
        else:
            (resp,content)=self.http.request(url)
        if resp.status==200 or returnError:
            return content
        elif resp.status==500 and addToken and content and content.find('ecode')!=-1 and int(CE_files.json_parse(content,'HB-EXCEPTION')['ecode'])==606:
            if count < 1: 
                self.__validate_token()
                return self.get_request(url,returnError=returnError,count=(count+1))
            print resp.status
            print content
            raise RuntimeError(content)
            return None
        else:
            print resp.status
            print content
            raise RuntimeError(content)
            return None
            


def main():
    test=CE_files()
    test.connect(sys.argv[1],sys.argv[2])
    print test.listFiles()
    print test._get_file_from_list(path='/dudyk-backup/deja')
#    test.createFile('deja',parentid="aLztT0b3f0vpgWgeGFVyOw",type=1)#nIjbQxpsPk5YJkUA-aSGeA") 
#    test.put_file('/home/dudy/diablo1.iso','if5EI_qZF2jnJZk5uXteZw')
    test.retrieve_file('if5EI_qZF2jnJZk5uXteZw','/home/dudy/diablo1.iso.pogo')
    print test.create_path(path='/dudyk-backup/deja-vu')
    print test.listFiles(parentId="aLztT0b3f0vpgWgeGFVyOw")#nIjbQxpsPk5YJkUA-aSGeA") 
    print test.listFiles(parentId="nIjbQxpsPk5YJkUA-aSGeA")
    test2=CE_files("https://mycloud.bezeq.co.il/svc/api")


if __name__ == "__main__":
    main()

# vi: shiftwidth=4 tabstop=4 softtabstop=4 expandtab
