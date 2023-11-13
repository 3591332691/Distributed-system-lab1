package impl;
//TODO: your implementation
import api.NameNodePOA;
import utils.D_File;
public class NameNodeImpl extends NameNodePOA {
    private static final String FS_IMAGE_PATH = "FsImage.txt";
    private FileSystem fileSystem;
    private Map<Long, String> fileDescriptorMap; // 文件描述符和文件内容的映射表
    public NameNodeImpl() {
        String currentDirectory = System.getProperty("user.dir");
        String fsImagePath = currentDirectory + File.separator + "FsImage";
        File fsImageFile = new File(fsImagePath);
        fileSystem = new FileSystem;
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
        long maxFd = Long.MIN_VALUE;
        for (long fd : fileDescriptorMap.keySet()) {
            if (fd > maxFd) {
                maxFd = fd;
            }
        }
        
        return maxFd+1;
    }
    private void createFile(String filepath) {//创造文件的时候不用写分配策略
        try {
            D_File new_file = new D_File(filepath);
            new_file.setCreationTime(System.currentTimeMillis());//存的是一个毫秒数
            string fileContents =new_file.getFileContent();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FS_IMAGE_PATH, true), StandardCharsets.UTF_8));//TODO:记得把FS_IMAGE_PATH变成后缀txt
            writer.write(fileContents);
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
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
    // for write
    private D_File reBuildD_File(string fileContent) {
        String filepath;
        List<DataBlock> block_list;
        boolean write_permission;
        long file_size;
        long modification_time;
        long creation_time;
        //读取fileContent里面的值存到上面
        String[] parts = fileContent.split(";");
        //存filepath
        String[] fileParts = parts[0].split(":");
        filepath = fileParts.length > 1 ? filParts[1].trim() : "";
        //存block_list
        fileParts = parts[1].split(":");
        block_list = fileParts.length > 1 ? fileParts[1].trim() : "";
        //存write_permission
        //存file_size
        fileParts = parts[3].split(":");
        file_size = fileParts.length > 1 ? fileParts[1].trim() : "";
        //存modification_time
        fileParts = parts[4].split(":");
        modification_time = fileParts.length > 1 ? fileParts[1].trim() : "";
        //存creation_time
        fileParts = parts[5].split(":");
        creation_time = fileParts.length > 1 ? fileParts[1].trim() : "";
        //创造出要返回的D_File
        D_File new_file = new D_File(filepath);
        new_file.setWritePermission = true;
        new_file.setBlockList(block_list);
        new_file.setFileSize(FileSize);
        new_file.setModificationTime(modification_time);
        new_file.setCreationTime(creation_time);
        return new_file;
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
        List<DataBlock> occupiedBlocks;
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" + filepath)) {
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
                                    String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
                                    int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
                                    
                                    // 创建新的 DataBlock 对象并添加到 occupiedBlocks 列表
                                    occupiedBlocks.add(new DataBlock(dataNodeIdentifier, blockId));
                                }
                            }
                        } 
                    }
                    return line;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        int maxBlockIdNode1 = 0;
        int maxBlockIdNode2 = 0;
        List<DataBlock> blockToAppend;
        // 遍历 occupiedBlocks 列表
        for (DataBlock dataBlock : occupiedBlocks) {
            // 检查 dataNodeIdentifier 是否为 "datanode1"，并更新最大值
            if ("datanode1".equals(dataBlock.dataNodeIdentifier)) {
                maxBlockIdNode1 = Math.max(maxBlockIdNode1, dataBlock.blockId);
            }

            // 检查 dataNodeIdentifier 是否为 "datanode2"，并更新最大值
            if ("datanode2".equals(dataBlock.dataNodeIdentifier)) {
                maxBlockIdNode2 = Math.max(maxBlockIdNode2, dataBlock.blockId);
            }
        }
        double randomNumber = Math.random();
        if (randomNumber > 0.5) {
            // 选择dataNode1
            for(int i = 1;i<=n;i++){
                blockToAppend.add(new DataBlock("datanode1", maxBlockIdNode1+i));
            }
        } else {
            // 选择dataNode2
            for(int i = 1;i<=n;i++){
                blockToAppend.add(new DataBlock("datanode2", maxBlockIdNode2+i));
            }
        }
    }

    @Override
    public String open(String filepath, int mode) {//返回fd
        String fileContent = readFileFromFsImage(filepath);
        
        FileDesc FD = new FileDesc(fileDescriptor);
        if (fileContent != null && mode == 1) {
            // 文件已存在，只读，返回fd
            long fileDescriptor = generateUniqueFileDescriptor();//生成唯一的fd
            fileDescriptorMap.put(filepath, fileDescriptor);// 将文件路径和文件描述符存储到映射表中
            return FD.toString();
        } 
        else if(fileContent != null && mode == 0) {
            // 文件存在，要写，判断write permission
            String[] parts = fileContent.split(";");
            string writePermission ;
            for (String part : parts) {
            if (part.startsWith("write_permission:")) {
                writePermission = part.substring(part.indexOf(":") + 1);
                if(writePermission==1)return null;
                else{
                    long fileDescriptor = generateUniqueFileDescriptor();//生成唯一的fd
                    fileDescriptorMap.put(filepath, fileDescriptor);// 将文件路径和文件描述符存储到映射表中
                    return FD.toString();
                }
            } 
            }
        }
        else{
            //文件不存在,创建新文件
            createFile(filepath);
            fileContent = readFileFromFsImage(filepath);
            long fileDescriptor = generateUniqueFileDescriptor();//生成唯一的fd
            fileDescriptorMap.put(filepath, fileDescriptor);// 将文件路径和文件描述符存储到映射表中
            return FD.toString();
        }
        return null;
    }

    @Override
    public void close(String fd) {//在关闭文件的时候注销映射
        fileDescriptorMap.remove(fd);
    }
    
    @Override
    public void writeFile(in long fd, in byteArray bytes); {
        //在FSImage里写分配的结果，更新FsImage中block_list，file_size，modification_time
        // 根据文件描述符找到文件路径
        String fileContent = findFilepathByFileDescriptor(fd);
        // 检查文件路径是否有效
        if (filepath == null) {
            // 文件描述符无效，处理错误情况
            return;
        }
        List<DataBlock> block_listToAppend = new ArrayList<DataBlock>();
        D_File wf = reBuildD_File(fileContent);//把fileContent的数据注入到wf里面去
        // 算要写入的块数量
        int dataSize = bytes.length;
        dataSize = dataSize - (4096 - file_size%4096);
        if(dataSize>0){
            int numBlocks = calculateNumBlocks(dataSize);
            // 分配每一块在DataNode上的存储位置，记录在block_listToAppend里
            block_listToAppend.add(allocateBlock(numBlocks));//写这个分配的方法：随机
        }
        

        // 更新FsImage中的file_size和modification_time，block_list
        wf.setFileSize(wf.getFileSize()+dataSize);
        wf.setModificationTime(System.currentTimeMillis());
        wf.setBlockList(wf.getBlockList().addAll(blockListToAppend));
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line;
            StringBuilder fileContent  = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" + wf.getFilepath())) {
                    // 删掉原来那一行
                    continue;
                }
                fileContent.append(line).append("\n");
            }
            fileContent.append(wf.getFileContent()).append("\n");
            try (FileWriter writer = new FileWriter(FS_IMAGE_PATH)) {
                writer.write(fileContent.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    
    @Override
    public byteArray readFile(in long fd); {
        //TODO:返回block_list，让client自己去找dataNode拼接得到结果
        String fileContent = findFilepathByFileDescriptor(fd);
        // 检查文件路径是否有效
        if (filepath == null) {
            // 文件描述符无效，处理错误情况
            return;
        }
        D_File wf = reBuildD_File(fileContent);//把fileContent的数据注入到wf里面去
        byteArray result = new byteArray();
        for (Byte b : wf.getBlockList()) {
            result.add(b);
        }
        return result;
    }
}
