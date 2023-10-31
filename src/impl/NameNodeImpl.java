package impl;
//TODO: your implementation
import api.NameNodePOA;

public class NameNodeImpl extends NameNodePOA {
    private static final String FS_IMAGE_PATH = "FsImage";
    private FileSystem fileSystem;
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
    private void createFile(String filepath) {
        try {
            D_File new_file = new D_File(filepath);
            string fileContents = "filepath:"+new_file.getFilepath()+";"+"block_list:"+new_file.getBlockList()+";"+
            "write_permission:"+new_file.getWritePermission()+";"+"file_size:"+new_file.getFileSize()+";"+
            "modification_time:"+new_file.getModificationTime()+";"+"file_size:"+new_file.getFileSize()+";";
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FS_IMAGE_PATH, true), StandardCharsets.UTF_8));
            writer.write(fileContents);
            writer.newLine();  // 写入换行符
            writer.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String readFileFromFsImage(String filepath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(FS_IMAGE_PATH))) {
            String line,blockList,writePermission;
            while ((line = reader.readLine()) != null) {
                String decodedData = new String(line.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                if (line.startsWith("filepath:" + filepath)) {
                    // 返回文件内容
                    String[] parts = line.split(";");
                    for (String part : parts) {
                    if (part.startsWith("block_list:")) {
                        blockList = part.substring(part.indexOf(":") + 1);
                    } 
                    else if (part.startsWith("write_permission:")) {
                        writePermission = part.substring(part.indexOf(":") + 1);
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
    public String open(String filepath, int mode) {
        String fileContent = readFileFromFsImage(filepath);

        if (fileContent != null && mode == 1) {
            // 文件已存在，只读，返回文件内容
                    
            return fileContent;
        } 
        else if(fileContent != null && mode == 0) {
            // 文件存在，要写，判断write permission
            String[] parts = fileContent.split(";");
            string writePermission ;
            for (String part : parts) {
            if (part.startsWith("write_permission:")) {
                writePermission = part.substring(part.indexOf(":") + 1);
                if(writePermission==1)return null;
                else{return fileContent;}
            } 
            }
        }
        else{
            //文件不存在
            createFile(filepath);
            return null;
        }
        return null;
    }

    @Override
    public void close(String fileInfo) {

    }
}
