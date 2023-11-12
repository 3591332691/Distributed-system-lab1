package impl;
//TODO: your implementation
import api.NameNodePOA;
import utils.D_File;
public class NameNodeImpl extends NameNodePOA {
    private static final String FS_IMAGE_PATH = "FsImage";
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
    private void createFile(String filepath) {//TODO:创造文件的时候不用写分配策略
        try {
            D_File new_file = new D_File(filepath);
            string fileContents =new_file.getFileContent();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FS_IMAGE_PATH, true), StandardCharsets.UTF_8));
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
                    if (part.startsWith("file_size:")) {//以file_size结尾
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
        //TODO: 返回分配的结果，更新FsImage中block_list，file_size，modification_time
    


    }
    
    @Override
    public byteArray readFile(in long fd); {
        //TODO:返回block_list，让client自己去找dataNode拼接得到结果

    }
}
