package impl;

import api.NameNodePOA;
import utils.D_File;
import utils.DataBlock;
import utils.FileDesc;
import utils.FileSystem;

import java.util.HashMap;
import java.util.Map;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
public class NameNodeImpl extends NameNodePOA {
    private static final String FS_IMAGE_PATH = "FsImage.txt";
    private FileSystem fileSystem;
    private Map<Long, String> fileDescriptorMap = new HashMap<Long,String>(); // 文件描述符和文件内容的映射表
    public NameNodeImpl() {
        String currentDirectory = System.getProperty("user.dir");
        String fsImagePath = currentDirectory + File.separator + "FsImage.txt";
        File fsImageFile = new File(fsImagePath);
        fileSystem = new FileSystem();
        try {
            if (fsImageFile.createNewFile()) {
                System.out.println("FsImage file created successfully.");
            } else {
                System.out.println("FsImage file already exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //for OPEN
    private long generateUniqueFileDescriptor(){
        long maxFd = 0;
        if(fileDescriptorMap==null){}
        else{
            for (long fd : fileDescriptorMap.keySet()) {
                if (fd > maxFd) {
                    maxFd = fd;
                }
            }
        }

        
        return maxFd+1;
    }
    private void createFile(String filepath) {//创造文件的时候不用写分配策略
        try {
            D_File new_file = new D_File(filepath);
            new_file.setCreationTime(System.currentTimeMillis());//存的是一个毫秒数
            String fileContents =new_file.getFileContent();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FS_IMAGE_PATH, true), StandardCharsets.UTF_8));//TODO:记得把FS_IMAGE_PATH变成后缀txt
            writer.write(fileContents);//存入了FS就算是创建完成
            writer.newLine();  // 写入换行符
            writer.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String readFileFromFsImage(String filepath) {//从FsImage里读出file内容
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line;
            StringBuilder fileContent  = new StringBuilder();
            while ((line = reader.readLine()) != null) {

                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" + filepath)) {
                    // 返回文件内容
                    String[] parts = line.split(";");
                    for (String part : parts) {
                    if (part.startsWith("creation_time:")) {//以creation_time结尾
                        fileContent.append(part);
                        return fileContent.toString();
                    } 
                    else {
                        fileContent.append(part).append(System.lineSeparator());
                    }
                    }
                    return line;
                }
            }
            return "";
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
    // for write
    private D_File reBuildD_File(String fileContent) {
        String filepath;
        List<DataBlock> block_list;
        String block_list_s;
        boolean write_permission;
        long file_size;
        long modification_time;
        long creation_time;
        //读取fileContent里面的值存到上面
        if(fileContent==null||fileContent == "")return null;
        //TODO:这里要读FSImage把filePath=fileContent的文件内容读到fileContent里
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" +fileContent)) {
                    // 返回文件内容
                    fileContent = line;
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] parts = fileContent.split(";");
        if(parts.length >= 6){
            //存filepath
            String fileParts = parts[0];
            filepath = fileParts.substring("filepath:".length());
            //存block_list
            fileParts = parts[1];
            block_list_s = fileParts.substring("block_list:".length());
            //存write_permission
            //存file_size
            fileParts = parts[3];
            file_size = Long.parseLong(fileParts.substring("file_size:".length()));
            //存modification_time
            fileParts = parts[4];
            modification_time = Long.parseLong(fileParts.substring("modification_time:".length()));
            //存creation_time
            fileParts = parts[5];
            creation_time = Long.parseLong(fileParts.substring("creation_time:".length()));
            //创造出要返回的D_File
            D_File new_file = new D_File(filepath);
            new_file.setWritePermission (true);
            new_file.setBlockList2(block_list_s);
            new_file.setFileSize(file_size);
            new_file.setModificationTime(modification_time);
            new_file.setCreationTime(creation_time);
            return new_file;
        }
        return null;
    }
        // 计算需要的块数量
    private int calculateNumBlocks(int dataSize) {
        int blockSize = 4 * 1024;
        int numBlocks = dataSize / blockSize; 
        if (dataSize % blockSize != 0) {
            numBlocks++; // 如果数据大小不能完全被块大小整除，则增加一个块来容纳剩余的数据
        }
        return numBlocks;
    }
    private List<DataBlock> allocateBlock(int n){
        //遍历FsImage，把所有的被占据的块存到occupiedBlocks
        List<DataBlock> occupiedBlocks = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" )) {
                    // 返回文件内容
                    String[] parts = line.split(";");
                    for (String part : parts) {
                        if (part.startsWith("block_list:")) {//以block_list开头
                            String content = part.substring("block_list:".length());
                            String[] blockInfoArray = content.split("/");
                            // 遍历分割后的数组，创建新的 DataBlock 对象并添加到 occupiedBlocks 列表
                            for (String blockInfo : blockInfoArray) {
                                if (!blockInfo.trim().isEmpty()) {
                                    // 分割每个 DataBlock 的信息
                                    String[] infoArray = blockInfo.trim().split(",");
                                    // 提取 dataNodeIdentifier 和 blockId 的值
                                    if (infoArray.length > 1){
                                        String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                                        int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                                        occupiedBlocks.add(new DataBlock(dataNodeIdentifier, blockId));
                                    }
                                    // 创建新的 DataBlock 对象并添加到 occupiedBlocks 列表
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        int maxBlockIdNode0 = -1;
        int maxBlockIdNode1 = -1;
        List<DataBlock> blockToAppend = new ArrayList<DataBlock>();
        // 遍历 occupiedBlocks 列表
        for (DataBlock dataBlock : occupiedBlocks) {
            // 检查 dataNodeIdentifier 是否为 "datanode0"，并更新最大值
            if ("DataNode0".equals(dataBlock.getNodeIdentifier())) {
                maxBlockIdNode0 = Math.max(maxBlockIdNode0, dataBlock.getblock_id());
            }
            // 检查 dataNodeIdentifier 是否为 "datanode1"，并更新最大值
            if ("DataNode1".equals(dataBlock.getNodeIdentifier())) {
                maxBlockIdNode1 = Math.max(maxBlockIdNode1, dataBlock.getblock_id());
            }
        }
        double randomNumber = Math.random();
        if (randomNumber > 0.5) {
            // 选择dataNode0
            for(int i = 1;i<=n;i++){
                blockToAppend.add(new DataBlock("DataNode0", maxBlockIdNode0+i));
            }
        } else {
            // 选择dataNode1
            for(int i = 1;i<=n;i++){
                blockToAppend.add(new DataBlock("DataNode1", maxBlockIdNode1+i));
            }
        }
        return blockToAppend;
    }

    @Override
    public String open(String filepath, int mode) {//返回fd
        String fileContent = readFileFromFsImage(filepath);
        if (fileContent !="" && (mode ==1)) {
            // 文件已存在，只读，返回fd
            Long foundKey = null;
            long fileDescriptor;
            for (Map.Entry<Long, String> entry : fileDescriptorMap.entrySet()) {
                if (entry.getValue().equals(filepath)) {
                    foundKey = entry.getKey();
                    break;
                }
            }
            if(foundKey!=null) //如果找到已经有fd了，就用map里的fd
            {
                fileDescriptor = foundKey;//不用写映射
            }
            else{
                fileDescriptor = generateUniqueFileDescriptor();//生成唯一的fd
                fileDescriptorMap.put(fileDescriptor, filepath);//将文件路径和文件描述符存储到映射表中
            }
            FileDesc FD = new FileDesc();
            FD.fromString(String.valueOf(fileDescriptor));
            return FD.toString();
        } 
        else if(fileContent != "" && (mode == 3||mode==2)) {
            // 文件存在，要写，判断write permission
            String[] parts = fileContent.split(";");
            String writePermission ;
            for (String part : parts) {
            if (part.startsWith("write_permission:")) {
                writePermission = part.substring(part.indexOf(":") + 1);
                if(writePermission=="true")return null;
                else{//能写
                    Long foundKey = null;
                    Long fileDescriptor = null;
                    for (Map.Entry<Long, String> entry : fileDescriptorMap.entrySet()) {
                        if (entry.getValue().equals(filepath)) {
                            foundKey = entry.getKey();
                            break;
                        }
                    }
                    if(foundKey!=null) //如果找到已经有fd了，就用map里的fd
                    {
                        fileDescriptor = foundKey;//不用写映射
                    }
                    else {
                        fileDescriptor = generateUniqueFileDescriptor();//生成唯一的fd
                        fileDescriptorMap.put(fileDescriptor, filepath);//将文件路径和文件描述符存储到映射表中
                    }
                    //TODO：标注为已经在写
                    D_File wf = reBuildD_File(fileContent);//把fileContent的数据注入到wf里面去
                    wf.setWritePermission(true);//更新FsImage
                    try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
                        String line;
                        StringBuilder fileContent_  = new StringBuilder();
                        while ((line = reader.readLine()) != null) {
                            String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                            if (line.startsWith("filepath:" + wf.getFilepath())) {
                                // 删掉原来那一行
                                continue;
                            }
                            fileContent_.append(line).append("\n");
                        }
                        fileContent_.append(wf.getFileContent()).append("\n");
                        try (FileWriter writer = new FileWriter(FS_IMAGE_PATH)) {
                            writer.write(fileContent_.toString());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    FileDesc FD = new FileDesc();
                    FD.fromString(String.valueOf(fileDescriptor));//输入fd
                    return FD.toString();
                }
            } 
            }
        }
        else{
            //文件不存在,创建新文件
            createFile(filepath);
            //fileContent = readFileFromFsImage(filepath);
            long fileDescriptor = generateUniqueFileDescriptor();//生成唯一的fd
            FileDesc FD = new FileDesc();
            FD.fromString(String.valueOf(fileDescriptor));
            fileDescriptorMap.put(fileDescriptor,filepath );// 将文件路径和文件描述符存储到映射表中
            return FD.toString();
        }
        return null;
    }

    @Override
    public void close(String fd) {//在关闭文件的时候注销映射
        fileDescriptorMap.remove(Long.parseLong(fd));
    }
    
    @Override
    public void writeFile(int fd,  byte[] bytes){
        //在FSImage里写分配的结果，更新FsImage中block_list，file_size，modification_time
        // 根据文件描述符找到文件路径
        String fileContent = fileDescriptorMap.get(Long.valueOf(fd));
        // 检查文件路径是否有效
        if (fileContent == null) {
            // 文件描述符无效，处理错误情况
            return;
        }
        List<DataBlock> block_listToAppend = new ArrayList<DataBlock>();
        D_File wf = new D_File("fakePath");
        //
        wf = reBuildD_File(fileContent);//把fileContent的数据注入到wf里面去，wf现在是原来filepath的数据
        // 算要写入的块数量
        //TODO:要判断原来有没有block,没有的话对dataSizeToAppend进行判断
        int dataSizeToAppend = bytes.length;
        if(wf!=null&&wf.getFileSize()!=0&&(int)wf.getFileSize()%4096+dataSizeToAppend>4096) {
            dataSizeToAppend = dataSizeToAppend - (4096 - (int)wf.getFileSize()%4096);
        }
        if(dataSizeToAppend>0){//dataSizeToAppend现在是需要分配block的data大小
            int numBlocks = calculateNumBlocks(dataSizeToAppend);
            // 分配每一块在DataNode上的存储位置，记录在block_listToAppend里
            block_listToAppend=allocateBlock(numBlocks);//写这个分配的方法：随机
        }
        // 更新FsImage中的file_size和modification_time，block_list
        if(wf !=null){
            wf.setFileSize(wf.getFileSize()+bytes.length);
            wf.setModificationTime(System.currentTimeMillis());
            //wf.getBlockList2().addAll(block_listToAppend);
            List<DataBlock> a = wf.getBlockList2();
            if(a==null){
                wf.setBlockList(block_listToAppend);
            }
            else{
                a.addAll(block_listToAppend);
                wf.setBlockList(a);
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
                String line;
                StringBuilder fileContent_  = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    //String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                    if (line.startsWith("filepath:" + wf.getFilepath())) {
                        // 删掉原来那一行
                        continue;
                    }
                    fileContent_.append(line).append("\n");
                }
                fileContent_.append(wf.getFileContent()).append("\n");//加入新的（更新过blockList的）
                try (FileWriter writer = new FileWriter(FS_IMAGE_PATH)) {
                    writer.write(fileContent_.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    
    @Override
    public String readFile(int fd) {
        //TODO:返回block_list，让client自己去找dataNode拼接得到结果
        String fileContent = fileDescriptorMap.get(Long.valueOf(fd));
        // 检查文件路径是否有效
        if (fileContent == null) {return null;}
        D_File wf = reBuildD_File(fileContent);//把fileContent的数据注入到wf里面去
        if(wf!=null)return wf.getBlockList();
        else{
            return "";}
        
    }
}
