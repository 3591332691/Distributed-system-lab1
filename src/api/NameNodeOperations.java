package api;


public interface NameNodeOperations 
{

  //TODO: complete the interface design
  String open (String filepath, int mode);
  void writeFile (int fd, byte[] bytes);
  String readFile (int fd);
  void close (String fileInfo);
} // interface NameNodeOperations
