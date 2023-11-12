package impl;
//TODO: your implementation
import api.DataNodePOA;

public class DataNodeImpl extends DataNodePOA {
    private String dataNodeIdentifier; // 数据节点标识符
    private Map<Integer, Integer> blockIdToPhysicalAddressMap; // block_id 到物理地址的映射表
    public DataNodeImpl(String dataNodeIdentifier) {
        this.dataNodeIdentifier = dataNodeIdentifier;
        this.blockIdToPhysicalAddressMap = new HashMap<>();

        // 在这里初始化映射表并且创建实际存储的文件
        initializeMappingTable();
    }
    @Override
    public byte[] read(int block_id) {
        String physicalAddress = blockIdToPhysicalAddressMap.get(block_id);
        // 根据物理地址读取数据块内容
        byte[] blockData = readBlockFromPhysicalAddress(physicalAddress);
        return blockData;
        return new byte[0];
    }

    @Override
    public void append(int block_id, byte[] bytesToWrite) {
        // 检查指定的 block_id 是否存在于映射表中
    if (blockIdToPhysicalAddressMap.containsKey(block_id)) {
        // 获取指定 block_id 对应的起始物理地址
        int physicalAddress = blockIdToPhysicalAddressMap.get(block_id);
        
        String fileName = dataNodeIdentifier + ".bin";
        String filePath = "../disk/" + fileName;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // 设置文件指针位置为指定的起始物理地址
            file.seek(physicalAddress);

            // 写入数据,是直接覆盖
            file.write(bytes);
            
            System.out.println("Data appended to block_id " + block_id + " in file: " + fileName);
        } catch (IOException e) {
            System.out.println("An error occurred while appending data to the file: " + e.getMessage());
        }
    } else {
        System.out.println("Block_id " + block_id + " does not exist.");
    }
    }

    @Override
    public int randomBlockId() {//暂时不知道干什么用
        return 0;
    }
    private void initializeMappingTable() {
        //写block_id和offset的映射表内容
        int offset = 4096;
        for(int i = 0;i<100;i++){//初始化100个block_id to 文件里偏移量的映射
            blockIdToPhysicalAddressMap.put(i, i*offset);
        }
        //创建用于存储的物理文件
        String fileName = dataNodeIdentifier + ".bin";
        String filePath = "../disk/"+fileName;
        File file = new File(filePath);

        try {
            boolean created = file.createNewFile();
            
            if (created) {
                System.out.println("Binary file created successfully: " + fileName);
            } else {
                System.out.println("File already exists: " + fileName);
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the file: " + e.getMessage());
        }
    }

    private byte[] readBlockFromPhysicalAddress(String physicalAddress) {
    // 在这里实现根据物理地址读取数据块内容的逻辑
    byte[] blockData = null;
    String fileName = dataNodeIdentifier + ".bin";
    String filePath = "../disk/"+fileName;
    // 假设你使用一个文件输入流来读取数据块
    try (FileInputStream fis = new FileInputStream(filePath)) {
        // 设置读取位置为物理地址所指定的偏移量
        fis.skip(Long.parseLong(physicalAddress));
    
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
