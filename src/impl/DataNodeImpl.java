package impl;
//TODO: your implementation
import api.DataNodePOA;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.UUID;

public class DataNodeImpl extends DataNodePOA {
    private String dataNodeIdentifier; // 数据节点标识符
    private Map<Integer, String> blockIdToPhysicalAddressMap; // block_id 到物理地址的映射表
    public DataNodeImpl() {//String dataNodeIdentifier
        File directory = new File(System.getProperty("user.dir") + File.separator + "disk");
        File[] files = directory.listFiles();
        int maxNumber = -1;

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && file.getName().startsWith("DataNode")) {
                    String fileName = file.getName();
                    int number = Integer.parseInt(fileName.substring(8));
                    if (number > maxNumber) {
                        maxNumber = number;
                    }
                }
            }
        }
        dataNodeIdentifier = "DataNode" + (maxNumber + 1);


        this.blockIdToPhysicalAddressMap = new HashMap<>();
        //TODO：这里要创建文件夹
        String currentDirectory = System.getProperty("user.dir");
        String filePath = currentDirectory+File.separator+"disk" +File.separator+ dataNodeIdentifier;
        File fileCheck = new File(filePath);
        fileCheck.mkdir();
        System.out.println("测试");
    }
    @Override
    public byte[] read(int block_id) {
        String physicalAddress = blockIdToPhysicalAddressMap.get(block_id);
        // 根据物理地址读取数据块内容
        if (physicalAddress == null) {
            byte[] a = new byte[4096];
            return a;
        } else {
            byte[] blockData = readBlockFromPhysicalAddress(physicalAddress);
            return blockData;
        }


    }

    @Override
    public void append(int block_id, byte[] bytesToWrite) {
        System.out.println("进入data的append");
        // 检查指定的 block_id 是否存在于映射表中
        if (blockIdToPhysicalAddressMap.containsKey(block_id)) {
            //如果存在于映射表中， 获取指定 block_id 对应的起始物理地址
            System.out.println("block已经存在在映射表里");
            String physicalAddress = blockIdToPhysicalAddressMap.get(block_id);
            String filePath = physicalAddress;
            try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
                // 写入数据,是append
                file.seek(file.length());
                file.write(bytesToWrite);
            } catch (IOException e) {
                System.out.println("An error occurred while appending data to the file: " + e.getMessage());
            }
        } else {
            System.out.println("一个新的block.txt需要被创建");
            String currentDirectory = System.getProperty("user.dir");
            String filePath = currentDirectory+File.separator+"disk" +File.separator+ dataNodeIdentifier+File.separator+block_id+".txt";
            blockIdToPhysicalAddressMap.put(block_id, filePath);
            File fileCheck = new File(filePath);
            if (!fileCheck.exists()) {
                // block不存在，可以选择创建一个新文件
                try {
                    fileCheck.createNewFile();//
                } catch (IOException e) {
                    System.out.println("为什么找不到地址？");
                    System.out.println("An error occurred while creating the file: " + e.getMessage());
                }
            }
            try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
                // 写入数据,是append
                file.seek(file.length());
                file.write(bytesToWrite);
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
    try (FileInputStream fis = new FileInputStream(physicalAddress)) {//
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
