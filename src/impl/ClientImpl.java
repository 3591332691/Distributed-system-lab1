package impl;
//TODO: your implementation
import api.Client;
import api.DataNodeHelper;
import api.NameNodeHelper;
import api.NameNode;
import api.DataNode;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.NamingContextExt;
import org.omg.CosNaming.NamingContextExtHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.nio.file.Files;

public class ClientImpl implements Client{
    private static final String FS_IMAGE_PATH = "FsImage.txt";
    private static final int MAX_DATA_NODE = 2;
    private NameNode nameNode ;
    private DataNode[] dataNodes = new DataNode[MAX_DATA_NODE];
    private Map<Long, String> fileToDescriptorMap = new HashMap<Long,String>() ;
    private Map<Long, Integer> DescriptorMapToRW = new HashMap<Long,Integer>() ;
    public ClientImpl(){

        try{
            String[] args = {};
            Properties properties = new Properties();
            properties.put("org.omg.CORBA.ORBInitialHost","127.0.0.1");
            properties.put("org.omg.CORBA.ORBInitialPort","1050");
            //new ORB object
            ORB orb = ORB.init(args,properties);
            //Naming context
            org.omg.CORBA.Object objRef = orb.resolve_initial_references("NameService");
            NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);
            //obtain a remote object
            nameNode = NameNodeHelper.narrow(ncRef.resolve_str("NameNode"));
            for(int dataNodeId = 0;dataNodeId<MAX_DATA_NODE;dataNodeId++) {

                dataNodes[dataNodeId] = DataNodeHelper.narrow(ncRef.resolve_str("DataNode" + dataNodeId));
                System.out.println("DataNode" + dataNodeId + " is obtained.");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public int open(String filepath, int mode) {// 返回fd
        String temp = nameNode.open(filepath, mode);
        int fd = (int)Long.parseLong(temp);
        if(fileToDescriptorMap.containsKey(Long.valueOf(fd))) {
            DescriptorMapToRW.put(Long.valueOf(fd), mode);
            return fd;
        }
        fileToDescriptorMap.put(Long.valueOf(fd), filepath);
        DescriptorMapToRW.put(Long.valueOf(fd), mode);
        System.out.println("fd是"+fd);
        return fd;
    }

    @Override
    public void append(int fd, byte[] bytes) {
        if(DescriptorMapToRW.get(Long.valueOf(fd))==1){//只读的话
            System.out.println("you can only read,not write.");}
        else{
            System.out.println("you can write.");
            nameNode.writeFile(fd, bytes);//NameNode里面进行分配
            String filepath = fileToDescriptorMap.get(Long.valueOf(fd));
            //从FsImage中获取block信息
            try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
                String line;
                String block_list = "";
                while ((line = reader.readLine()) != null) {
                    //String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                    if(block_list!="")break;
                    if (line.startsWith("filepath:" + filepath)) {
                        // 返回文件内容
                        String[] parts = line.split(";");
                        for (String part : parts) {
                            if (part.startsWith("block_list:")) {
                                block_list=part;
                                break;
                            }
                        }
                    }

                }
                if(block_list.length()>"block_list:".length())
                    block_list = block_list.substring("block_list:".length());
                else{
                    System.out.println("Error, when writing, can not find the file in FSImage");
                }
                String[] blockInfoArray = block_list.split("/");//get blocks
                //查找哪个block_id是被写的第一个
                int startI = 0;
                for(int i = 0; i < blockInfoArray.length && blockInfoArray[i] != null;i++){
                    String[] infoArray = blockInfoArray[i].trim().split(",");
                    if(infoArray[0].trim().length()>"DataNode: ".length()&&infoArray[1].trim().length()>"BlockId: ".length()){
                        String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                        int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                        String currentDirectory = System.getProperty("user.dir");
                        String path = currentDirectory+ File.separator+"disk" +File.separator+ dataNodeIdentifier+File.separator+blockId+".txt";
                        //String path = "disk/" + dataNodeIdentifier+"/"+blockId+".txt";
                        Path filePath_ = Paths.get(path);
                        if (Files.exists(filePath_)) {
                            try {
                                long fileSize = Files.size(filePath_);
                                if(fileSize<4096){ startI = i;break;}//到要写的第一个block
                            } catch (Exception e) {
                                // 处理异常
                                e.printStackTrace();
                            }
                        } else {//刚好这个block是要写的第一个
                            startI = i;
                            break;
                        }
                    }
                }
                //从要写的第一个block开始
                for(int i = startI; (i < blockInfoArray.length) && (blockInfoArray[i] != null); i++){
                    String[] infoArray = blockInfoArray[i].trim().split(",");
                    if(infoArray[0].trim().length()>"DataNode: ".length()&&infoArray[1].trim().length()>"BlockId: ".length()){
                        String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                        int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                        //打开新的block
                        String currentDirectory = System.getProperty("user.dir");
                        String path = currentDirectory+ File.separator+"disk" +File.separator+ dataNodeIdentifier+File.separator+blockId+".txt";

                        //String path = "disk/" + dataNodeIdentifier+"/"+blockId+".txt";
                        Path filePath_ = Paths.get(path);
                        if (Files.exists(filePath_)) {
                            try {
                                long fileSize = Files.size(filePath_);
                                if(fileSize<4096 && bytes.length>=(4096-fileSize)){
                                    if(dataNodeIdentifier == "DataNode0"){
                                        dataNodes[0].append(blockId,Arrays.copyOfRange(bytes, 0, (int)(4096 - fileSize)));
                                        bytes = Arrays.copyOfRange(bytes, (int)(4096 - fileSize), bytes.length);
                                    }
                                    else if(dataNodeIdentifier == "DataNode1"){
                                        dataNodes[1].append(blockId,Arrays.copyOfRange(bytes, 0, (int)(4096 - fileSize)));
                                        bytes = Arrays.copyOfRange(bytes, (int)(4096 - fileSize), bytes.length);
                                    }
                                }
                                else if(fileSize<4096 && bytes.length<=(4096-fileSize)){
                                    if(dataNodeIdentifier == "DataNode0"){
                                        dataNodes[0].append(blockId,bytes);
                                    }
                                    else if(dataNodeIdentifier == "DataNode1"){
                                        dataNodes[1].append(blockId,bytes);
                                    }
                                }
                            } catch (Exception e) {
                                // 处理异常
                                e.printStackTrace();
                            }
                        } else {//new here
                            if(bytes.length>4096){
                                if(dataNodeIdentifier == "DataNode0"){
                                    dataNodes[0].append(blockId,Arrays.copyOfRange(bytes, 0, 4096));
                                    bytes = Arrays.copyOfRange(bytes, 4096, bytes.length);
                                }
                                else if(dataNodeIdentifier == "DataNode1"){
                                    dataNodes[1].append(blockId,Arrays.copyOfRange(bytes, 0, 4096));
                                    bytes = Arrays.copyOfRange(bytes, 4096, bytes.length);
                                }
                            }
                            else{
                                if(dataNodeIdentifier == "DataNode0"){
                                    dataNodes[0].append(blockId,Arrays.copyOfRange(bytes, 0, bytes.length));
                                }
                                else if(dataNodeIdentifier == "DataNode1"){
                                    dataNodes[1].append(blockId,Arrays.copyOfRange(bytes, 0,bytes.length));
                                }
                            }

                        }
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    @Override
    public byte[] read(int fd) {
         String block_list = nameNode.readFile(fd);
         if(block_list.contains("BlockId")){return "".getBytes(StandardCharsets.UTF_8);}
         String[] blockInfoArray = block_list.split("/");
         if(blockInfoArray.length<=1){return "".getBytes(StandardCharsets.UTF_8);}
         List<byte[]> resultList = new ArrayList<>();//datanode 的 read返回值类型是byte[]
         for(int i = 0; i<blockInfoArray.length&&blockInfoArray[i]!=null;i++){
                String[] infoArray = blockInfoArray[i].trim().split(",");
                if(infoArray[0].trim().length()>="DataNode: ".length()&&infoArray[1].trim().length()>="BlockId: ".length()){
                    String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                    int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                    if(dataNodeIdentifier=="datanode1"){
                        resultList.add(dataNodes[0].read(blockId));
                    }
                    else if(dataNodeIdentifier=="datanode2"){
                        resultList.add(dataNodes[1].read(blockId));
                    }
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
        nameNode.close(String.valueOf(fd));
        fileToDescriptorMap.remove(Long.valueOf(fd));
    }
}
