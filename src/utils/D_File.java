import java.util.List;

public class D_File {
    private String filepath;
    private List<String> block_list;
    private boolean write_permission;
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

    public List<String> getBlockList() {
        return block_list;
    }

    public void setBlockList(List<String> block_list) {
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
    public long getFileContent() {
        string temp = "filepath:"+new_file.getFilepath()+";"+"block_list:"+new_file.getBlockList()+";"+
            "write_permission:"+new_file.getWritePermission()+";"+"file_size:"+new_file.getFileSize()+";"+
            "modification_time:"+new_file.getModificationTime()+";"+"file_size:"+new_file.getFileSize()+";";
        return temp;
    }
}