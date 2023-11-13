import java.util.List;
public class DataBlock{//代表一个数据块的地址
    String dataNodeIdentifier;
    int block_id;
    public DataBlock(String dataNodeIdentifier, int blockId) {
        this.dataNodeIdentifier = dataNodeIdentifier;
        this.blockId = blockId;
    }
}
public class D_File {//代表文件
    private String filepath;
    private List<DataBlock> block_list;
    private boolean write_permission;//true代表正在被写入
    private long file_size;
    private long modification_time;
    private long creation_time;

    public D_File(String filepath) {
        this.filepath = filepath;
        this.file_size = 0;
        LocalDateTime now = LocalDateTime.now();
        this.creation_time = now;
    }

    // Getter and setter methods

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public List<DataBlock> getBlockList() {
        StringBuilder result = new StringBuilder();

        for (DataBlock dataBlock : block_list) {
            result.append("DataNode: ").append(dataBlock.dataNodeIdentifier)
                .append(", BlockId: ").append(dataBlock.blockId)
                .append("/"); // 用/来表示block之间的分割
        }

        return result.toString();
    }

    public void setBlockList(List<DataBlock> block_list) {
        this.block_list = block_list;
    }

    public boolean hasWritePermission() {
        return write_permission;
    }

    public void setWritePermission(boolean write_permission) {
        this.write_permission = write_permission;
    }

    public long getFileSize() {
        return file_size;
    }

    public void setFileSize(long file_size) {
        this.file_size = file_size;
    }

    public long getModificationTime() {
        return modification_time;
    }

    public void setModificationTime(long modification_time) {
        this.modification_time = modification_time;
    }

    public long getCreationTime() {
        return creation_time;
    }

    public void setCreationTime(long creation_time) {
        this.creation_time = creation_time;
    }
    public String getFileContent() {
        string temp = "filepath:"+new_file.getFilepath()+";"+"block_list:"+new_file.getBlockList()+";"+
            "write_permission:"+new_file.getWritePermission()+";"+"file_size:"+new_file.getFileSize()+";"+
            "modification_time:"+new_file.getModificationTime()+";"+"creation_time:"+new_file.getCreationTime()+";";
        return temp;
    }
}