package utils;
import java.util.ArrayList;
import java.util.List;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
public class D_File {//代表文件
    private String filepath;
    public List<DataBlock> block_list;
    private boolean write_permission;//true代表正在被写入
    private long file_size;
    private long modification_time;
    private long creation_time;

    public D_File(String filepath) {
        this.filepath = filepath;
        this.file_size = 0;
        LocalDateTime now = LocalDateTime.now();
        this.creation_time = now.toEpochSecond(ZoneOffset.UTC);
    }

    // Getter and setter methods

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getBlockList() {
        StringBuilder result = new StringBuilder();
        if(block_list==null|| block_list.size() == 0)return "";
        else{
            for (DataBlock dataBlock : block_list) {
                result.append("DataNode: ").append(dataBlock.dataNodeIdentifier)
                        .append(", BlockId: ").append(dataBlock.block_id)
                        .append("/"); // 用/来表示block之间的分割
            }
            return result.toString();
        }


    }
    public List<DataBlock> getBlockList2() {
        return block_list;
    }

    public void setBlockList(List<DataBlock> block_list) {
        this.block_list = block_list;
    }
    public void setBlockList2(String block_list) {//string to block
        List<DataBlock> temp = new ArrayList<DataBlock>();
        String[] blockInfoArray = block_list.split("/");
        if(blockInfoArray.length<=1)return ;
        for(int i = 0;blockInfoArray[i]!=null||i<blockInfoArray.length-1;i++){
            String[] infoArray = blockInfoArray[i].trim().split(",");
            String dataNodeIdentifier = infoArray[0].trim().substring("DataNode: ".length());
            int blockId = Integer.parseInt(infoArray[1].trim().substring("BlockId: ".length()));
            DataBlock a = new DataBlock(dataNodeIdentifier,blockId);
            temp.add(a);
        }
        this.block_list = temp;
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
        String temp = "filepath:"+getFilepath()+";"+"block_list:"+getBlockList()+";"+
            "write_permission:"+hasWritePermission()+";"+"file_size:"+getFileSize()+";"+
            "modification_time:"+getModificationTime()+";"+"creation_time:"+getCreationTime()+";";
        return temp;
    }
}