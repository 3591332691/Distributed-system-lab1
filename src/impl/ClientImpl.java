package impl;
//TODO: your implementation
import api.Client;
public class ClientImpl implements Client{
    private static final String FS_IMAGE_PATH = "FsImage.txt";
    private static final int MAX_DATA_NODE = 2;
    private NameNode nameNode;
    private DataNode[] dataNode = new DataNode[MAX_DATA_NODE];
    private Map<Long, String> fileToDescriptorMap;
    public ClientImpl(){
        dataNode[0] = new DataNode("datanode1");
        dataNode[1] = new DataNode("datanode2");
    }
    @Override
    public int open(String filepath, int mode) {// 返回fd
        int fd = nameNode.open(filepath, mode);
        fileToDescriptorMap.put(fd, filepath);
        return fd;
    }

    @Override
    public void append(int fd, byte[] bytes) {
        nameNode.writeFile(fd, bytes);
        String filepath = fileToDescriptorMap.get(fd);
        //TODO:从FsImage中获取block信息，找到上次写到哪里
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line;
            StringBuilder block_list  = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" + filepath)) {
                    // 返回文件内容
                    String[] parts = line.split(";");
                    for (String part : parts) {
                    if (part.startsWith("block_list:")) {
                        block_list.append(part);
                        break;
                    } 
                    }
                }
            }
            block_list = block_list.substring("block_list:".length());
            String[] blockInfoArray = block_list.split("/");
            //查找哪个block_id是被写的第一个
            int startI; 
            for(int i = 0; blockInfoArray[i]!=null;i++){
                String[] infoArray = blockInfoArray[i].trim().split(",");
                String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                String path = "../disk/" + dataNodeIdentifier+"/"+block_id+".txt";
                Path filePath_ = Paths.get(path);
                if (Files.exists(filePath_)) {
                    try {
                        long fileSize = Files.size(filePath_);
                        if(fileSize<4096){ startI = i;break;}
                    } catch (Exception e) {
                        // 处理异常
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("File does not exist.");
                }
            }
            //
            for(int i =startI; blockInfoArray[i]!=null;i++){
                String[] infoArray = blockInfoArray[i].trim().split(",");
                String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                String path = "../disk/" + dataNodeIdentifier+"/"+block_id+".txt";
                Path filePath_ = Paths.get(path);
                if (Files.exists(filePath_)) {
                    try {
                        long fileSize = Files.size(filePath_);
                        if(fileSize<4096 && bytes.length>=(4096-fileSize)){ 
                            if(dataNodeIdentifier == "datanode1"){
                                dataNode[0].append(blockId,Arrays.copyOfRange(bytes, 0, (4096 - fileSize)));
                                bytes = Arrays.copyOfRange(bytes, (4096 - fileSize), bytes.length);
                            }
                            else if(dataNodeIdentifier == "datanode2"){
                                dataNode[1].append(blockId,Arrays.copyOfRange(bytes, 0, (4096 - fileSize)));
                                bytes = Arrays.copyOfRange(bytes, (4096 - fileSize), bytes.length);
                            }
                        }
                        else if(fileSize<4096 && bytes.length<=(4096-fileSize)){
                            if(dataNodeIdentifier == "datanode1"){
                                dataNode[0].append(blockId,bytes);
                            }
                            else if(dataNodeIdentifier == "datanode2"){
                                dataNode[1].append(blockId,bytes);
                            }
                        }
                    } catch (Exception e) {
                        // 处理异常
                        e.printStackTrace();
                    }
                } else {
                    if(bytes.length>4096){
                        if(dataNodeIdentifier == "datanode1"){
                            dataNode[0].append(blockId,Arrays.copyOfRange(bytes, 0, 4096));
                            bytes = Arrays.copyOfRange(bytes, 4096, bytes.length);
                        }
                        else if(dataNodeIdentifier == "datanode2"){
                            dataNode[1].append(blockId,Arrays.copyOfRange(bytes, 0, 4096));
                            bytes = Arrays.copyOfRange(bytes, 4096, bytes.length);
                    }
                    }
                    else{
                        if(dataNodeIdentifier == "datanode1"){
                            dataNode[0].append(blockId,Arrays.copyOfRange(bytes, 0, bytes.length));
                        }
                        else if(dataNodeIdentifier == "datanode2"){
                            dataNode[1].append(blockId,Arrays.copyOfRange(bytes, 0,bytes.length));
                        }
                    }
                    
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public byte[] read(int fd) {
         byteArray input = nameNode.readFile(fd);
         String str = new String(input, "UTF-8");
         block_list = str.substring("block_list:".length());
         String[] blockInfoArray = block_list.split("/");
         List<byte[]> resultList = new ArrayList<>()
         for(int i = 0; blockInfoArray[i]!=null;i++){
                String[] infoArray = blockInfoArray[i].trim().split(",");
                String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                if(dataNodeIdentifier=="datanode1"){
                    resultList.add(dataNode[0].read());
                }
                else if(dataNodeIdentifier=="datanode2"){
                    resultList.add(dataNode[1].read());
                }
        }
        int totalSize = resultList.stream().mapToInt(arr -> arr.length).sum();
        byte[] combinedResult = new byte[totalSize];

        int currentIndex = 0;
        for (byte[] result : resultList) {
            System.arraycopy(result, 0, combinedResult, currentIndex, result.length);
            currentIndex += result.length;
        }

        return combinedResult;

    }

    @Override
    public void close(int fd) {
        nameNode.close(fd);
        fileToDescriptorMap.remove(fd);
    }
}
