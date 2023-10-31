package api;


/**
* api/NameNodeOperations.java .
* ��IDL-to-Java ������ (����ֲ), �汾 "3.2"����
* ��api.idl
* 2023��10��31�� ���ڶ� ����01ʱ51��04�� CST
*/

public interface NameNodeOperations 
{

  //TODO: complete the interface design
  String open (String filepath, int mode);
  void writeFile (int fd, byte[] bytes);
  byte[] readFile (int fd);
  void close (String fileInfo);
} // interface NameNodeOperations
