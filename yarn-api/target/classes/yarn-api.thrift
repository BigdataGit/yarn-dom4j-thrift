namespace java com.sf.yarn.service

struct QueueParam{
  1: string queueName;
  2: string minResources;
  3: string maxResources;
  4: string maxRunningApps;
}

service YarnApiService{

  bool addQueueResource(1:list<QueueParam>paras)

  bool delQueueResource(1:list<string> queueName)

  bool updateQueueResource(1:list<QueueParam>paras)

}