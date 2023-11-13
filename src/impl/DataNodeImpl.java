package impl;
//TODO: your implementation
import api.DataNodePOA;

public class DataNodeImpl extends DataNodePOA {
    private String dataNodeIdentifier; // 数据节点标识符
    private Map<Integer, String> blockIdToPhysicalAddressMap; // block_id 到物理地址的映射表
    public DataNodeImpl(String dataNodeIdentifier) {
        this.dataNodeIdentifier = dataNodeIdentifier;
        this.blockIdToPhysicalAddressMap = new HashMap<>();
    }
    @Override
    public byte[] read(int block_id) {
        String physicalAddress = blockIdToPhysicalAddressMap.get(block_id);
        // 根据物理地址读取数据块内容
        byte[] blockData = readBlockFromPhysicalAddress(physicalAddress);
        return blockData;
    }

    @Override
    public void append(int block_id, byte[] bytesToWrite) {
        // 检查指定的 block_id 是否存在于映射表中
        if (blockIdToPhysicalAddressMap.containsKey(block_id)) {
            // 获取指定 block_id 对应的起始物理地址
            String physicalAddress = blockIdToPhysicalAddressMap.get(block_id);
            
            String filePath = "../disk/" + dataNodeIdentifier+"/"+block_id+".txt";

            try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
                // 写入数据,是append
                file.seek(file.length());
                file.write(bytes);
                
            } catch (IOException e) {
                System.out.println("An error occurred while appending data to the file: " + e.getMessage());
            }
        } else {
            String filePath = "../disk/" + dataNodeIdentifier+"/"+block_id+".txt";
            blockIdToPhysicalAddressMap.put(block_id, filePath);
            File fileCheck = new File(filePath);
            if (!fileCheck.exists()) {
                // 文件不存在，可以选择创建一个新文件
                try {
                    fileCheck.createNewFile();
                } catch (IOException e) {
                    System.out.println("An error occurred while creating the file: " + e.getMessage());
                }
            }
            try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
                // 写入数据,是append
                file.seek(file.length());
                file.write(bytes);
                
            } catch (IOException e) {
                System.out.println("An error occurred while appending data to the file: " + e.getMessage());
            }
        }
    }

    @Override
    public int randomBlockId() {//暂时不知道干什么用，分配在nameNode里进行了
        return 0;
    }
   
    private byte[] readBlockFromPhysicalAddress(String physicalAddress) {
    // 在这里实现根据物理地址读取数据块内容的逻辑
    byte[] blockData = null;
    // 假设你使用一个文件输入流来读取数据块
    try (FileInputStream fis = new FileInputStream(physicalAddress)) {
        // 每个数据块的大小是固定的 4096 字节
        byte[] buffer = new byte[4096];
        
        // 读取数据块内容
        int bytesRead = fis.read(buffer);
        
        // 根据实际读取的字节数创建一个新的字节数组来存储数据块内容
        blockData = new byte[bytesRead];
        System.arraycopy(buffer, 0, blockData, 0, bytesRead);
    } catch (IOException e) {
        e.printStackTrace();
    }
    
    return blockData;
}
}
