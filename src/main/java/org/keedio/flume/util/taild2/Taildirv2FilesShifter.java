package org.keedio.flume.util.taild2;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.keedio.flume.source.watchdir.InodeInfo;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.log4j.Logger;

/**
 * Created by luislazaro on 5/12/17.
 * lalazaro@keedio.com
 * Keedio
 */
public class Taildirv2FilesShifter {
    final static Logger logger = Logger.getLogger(Taildirv2FilesShifter.class);

    public static void main(String[] args) {
        String pathToOriginalFiles = args[0];
        String pathToProcessedFiles = args[1];
        String finalPath = args[2];
        Map<String, Long> originalFilesMap = new HashMap<>();
        Map<String, Long> processedFilesMap = new HashMap<>();
        try {
            logger.info("Watching path :" + pathToProcessedFiles);
            watchDir(pathToOriginalFiles,
                    pathToProcessedFiles,
                    originalFilesMap,
                    processedFilesMap,
                    finalPath);
        } catch (IOException e) {
            logger.error(e);
        } catch (ClassCastException e) {
            logger.error(e);
        } catch (ClassNotFoundException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    public static void watchDir(String pathToOriginalFiles,
                                String pathToProcessedFiles,
                                Map<String, Long> originalFilesMap,
                                Map<String, Long> processedFilesMap,
                                String finalPath) throws IOException, InterruptedException, ClassNotFoundException {
        Path path = Paths.get(pathToProcessedFiles);
        FileSystem fileSystem = FileSystems.getDefault();
        WatchService watchService = fileSystem.newWatchService();
        path.register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE);
        while (true) {
            WatchKey watchKey = watchService.take();
            List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
            for (WatchEvent<?> we : watchEvents) {
                if (we.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    logger.info("Created: " + we.context());
                    String filename = we.context().toString();
                    boolean status = processEvent(filename, pathToOriginalFiles, pathToProcessedFiles, originalFilesMap, processedFilesMap, finalPath);
                    if (status == false) {
                        logger.info("processEvent returned false, resetting");
                        watchKey.reset();
                    }
                } else if (we.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                    System.out.println("Deleted: " + we.context());
                } else if (we.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                    logger.info("Modified: " + we.context());
                    String filename = we.context().toString();
                    boolean status = processEvent(filename, pathToOriginalFiles, pathToProcessedFiles, originalFilesMap, processedFilesMap, finalPath);
                    if (status == false) {
                        logger.info("processEvent returned false, resetting");
                        watchKey.reset();
                    }
                }
                if (!watchKey.reset()) {
                    logger.info("breaking signal");
                    break;
                }
            }
        }
    }

    /**
     * Contador de lineas de fichero.
     *
     * @param pathtoFile
     * @return
     */

    public static Long getLinesFromFile(String pathtoFile) {
        long counter = 0L;
        try (BufferedReader in = new BufferedReader(new FileReader(pathtoFile))) {
            String line = null;
            while ((line = in.readLine()) != null) {
                counter++;
            }
        } catch (IOException e) {
        }
        return counter;
    }


    public static boolean processEvent(String filename,
                                       String pathToOriginalFiles,
                                       String pathToProcessedFiles,
                                       Map<String, Long> originalFilesMap,
                                       Map<String, Long> processedFilesMap,
                                       String finalPath) throws ClassNotFoundException {

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(pathToProcessedFiles + "/" + filename);
        } catch (FileNotFoundException e){
            logger.error(e);
        }
        Map<String, InodeInfo> map = new HashMap<>();
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(fis);
            //map = (Map<String, InodeInfo>) ois.readObject();
            map = (HashMap) ois.readObject();
        } catch (EOFException eof){
            logger.error(eof);
            //return false;
        } catch (IOException e){
            logger.error(e);
        }


        Set<String> elements = map.keySet();
        for (String element : elements) {
            processedFilesMap.put(map.get(element).getFileName(), map.get(element).getPosition());
        }
        Collection<File> files = FileUtils.listFiles(new File(pathToOriginalFiles), FileFilterUtils.trueFileFilter(), null);
        originalFilesMap.clear();
        for (File file : files) {
            Long lines = getLinesFromFile(file.getAbsolutePath());
            originalFilesMap.put(file.getAbsolutePath(), lines);
        }

        logger.info("originalFilesMap: " + originalFilesMap);
        logger.info("processedFilesMap: " + processedFilesMap);

        MapDifference<String, Long> mapDifference = Maps.difference(originalFilesMap, processedFilesMap);
        Map<String, Long> entriesInCommon = mapDifference.entriesInCommon();
        if (entriesInCommon.isEmpty()) {
            logger.info("No more files processed, nothing to move");
        } else {
            for (String pathTofile : entriesInCommon.keySet()) {
                if (new File(pathTofile).exists()) {
                    logger.info("Moving file " + pathTofile + " to " + finalPath);
                    try {
                        FileUtils.moveFileToDirectory(new File(pathTofile), new File(finalPath), false);
                    } catch (IOException e){
                        logger.error(e);
                    }
                } else {
                    logger.info("Cannot move " + pathTofile + " to " + finalPath + " ,already moved?");
                }
            }
        }

//        try {
//            fis.close();
//            ois.close();
//        } catch (IOException e){
//            logger.error(e);
//        }
        return true;
    }

}

