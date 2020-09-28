package com.vmware.supercollider;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AtlasLineageWatch {
    private static final Logger LOG = LogManager.getLogger(AtlasLineageWatch.class);

    private static boolean isSwpPath(Path path){
        return path.toString().startsWith(".") && path.toString().endsWith(".swp");
    }

    private static void runImpalaBridge(String bbkImportFilePath){
        try{
            LOG.info("Executing BBK Import File...");
            ProcessBuilder pb = new ProcessBuilder(bbkImportFilePath);
            Process p = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null)
            {
                LOG.info("BBK Import File Log: " + line);
            }
            LOG.info("BBK Import File finished execution");
        } catch (Exception ie) {
            ie.printStackTrace();
        }
    }

    public static void watchDirectoryPath(Path path, String bbkImportFilePath) {
        // Sanity check - Check if path is a folder
        try {
            Boolean isFolder = (Boolean) Files.getAttribute(path,
                    "basic:isDirectory", NOFOLLOW_LINKS);
            if (!isFolder) {
                throw new IllegalArgumentException("Path: " + path
                        + " is not a folder");
            }
        } catch (IOException ioe) {
            // Folder does not exists
            ioe.printStackTrace();
        }

        LOG.info("Watching path: " + path);

        // We obtain the file system of the Path
        FileSystem fs = path.getFileSystem();

        // We create the new WatchService using the new try() block
        try (WatchService service = fs.newWatchService()) {

            path.register(service, ENTRY_CREATE, ENTRY_MODIFY);

            // Start the infinite polling loop
            WatchKey key = null;
            while (true) {
                key = service.take();

                // Dequeueing events
                Kind<?> kind = null;
                for (WatchEvent<?> watchEvent : key.pollEvents()) {
                    // Get the type of the event
                    kind = watchEvent.kind();
                    if (OVERFLOW == kind) {
                        continue; // loop
                    } else if (ENTRY_CREATE == kind) {
                        // A new Path was created
                        Path newPath = ((WatchEvent<Path>) watchEvent)
                                .context();
                        if(isSwpPath(newPath)){
                            continue;
                        }
                        LOG.info("New path created:" + newPath);
                        runImpalaBridge(bbkImportFilePath);
                    } else if (ENTRY_MODIFY == kind) {
                        // modified
                        Path newPath = ((WatchEvent<Path>) watchEvent)
                                .context();
                        if(isSwpPath(newPath)){
                            continue;
                        }
                        LOG.info("New path modified:" + newPath);
                        runImpalaBridge(bbkImportFilePath);
                    }
                }

                if (!key.reset()) {
                    break; // loop
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

    }
    /*
     * Command-line interface.
     * @param args[0]: Absolute Path to BBK Impala Bridge script
     * @param args[1]: Watch Folder
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
        BasicConfigurator.configure();
        File dir = new File(args[0]);
        watchDirectoryPath(dir.toPath(), args[1]);
    }
}