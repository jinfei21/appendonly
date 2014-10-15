package com.ctriposs.quickcache.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.Files.isSymbolicLink;

public class FileUtil {

	private static final int BUFFER_SIZE = 4096 * 4;
	
	/**
	 * Only check if a given filename is valid according to the OS rules.
	 * 
	 * You still need to handle other failures when actually creating 
	 * the file (e.g. insufficient permissions, lack of drive space, security restrictions). 
	 * @param file the name of a file
	 * @return true if the file is valid, false otherwise
	 */
	public static boolean isFilenameValid(String file) {
		File f = new File(file);
		try {
			f.getCanonicalPath();
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	

    public static List<File> listFiles(File dir){
    	List<File> list = new ArrayList<File>();
    	if(dir.exists()) {
    		for (File f : dir.listFiles()) {
    			if(f.isFile()) {
    				list.add(f);
    			}
    		}
    	}
    		
    	return list;
    }
    
    public static File rename(String from,String to) {
    	File file = new File(from);
    	file.renameTo(new File(to));
    	return file;
    }

    /**
     * Deletes a directory recursively.
     * @param directory directory to delete
     * @throws java.io.IOException in case deletion is unsuccessful
     */
    public static void deleteDirectory(final File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        if (!isSymbolicLink(directory.toPath())) {
            cleanDirectory(directory);
        }

        if (!directory.delete()) {
            final String message = "Unable to delete directory " + directory + ".";
            throw new IOException(message);
        }
    }

    /**
     * Clean a directory without delete it
     * @param directory directory to clean
     * @throws java.io.IOException in case cleaning is unsuccessful
     */
    public static void cleanDirectory(final File directory) throws IOException {
        if (!directory.exists()) {
            final String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            final String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        final File[] files = directory.listFiles();
        if (files == null) {    // null if security restricted
            throw new IOException("Failed to list content of " + directory);
        }

        IOException exception = null;
        for (final File file : files) {
            try {
                forceDelete(file);
            } catch (final IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

        /**
         * Deletes a file. If file is a directory, delete it and all sub-directories.
         * @param file file or directory to delete, must not be {@code null}
         */
    public static void forceDelete(final File file) throws IOException {
        if (file == null) {
            return;
        }

        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            final boolean filePresent = file.exists();
            if (!file.delete()) {
                if (!filePresent) {
                    throw new FileNotFoundException("File not exist " + file);
                }

                final String message = "Unable to delete file " + file;
                throw new IOException(message);
            }
        }
    }
}
