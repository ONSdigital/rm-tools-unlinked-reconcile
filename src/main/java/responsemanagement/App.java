package responsemanagement;

import com.jcraft.jsch.*;
import com.opencsv.CSVReader;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Unlinked Case Reference Reconciler
 *
 */
public class App {

    //SFTP Information
    private static String sftpHost;
    private static String sftpUsername;
    private static String sftpPassword;

    //File Locations
    private static String fileInputLocation;
    private static String fileOutputLocationPaperReceipt;
    private static String fileOutputLocationLinkedCaseRefsMaster;
    private static String fileOutputLocationLocal;

    //File Names
    private static String fileNameCaseRefsToCheck;
    private static String fileNameCaseRefExtract;
    private static String fileNameUnlinkedCaseReceiptExtract;
    private static String fileNameDrsReport;
    private static String fileNameLinkedCaseRefsMaster;

    public static void main( String[] args ) {

        try {

            importFileNamesAndLocationsFromPropertyFile();

            createCaseRefsFromExtractToCheck();

            //Matches Unlinked CaseRefs from DRS with CaseRefs from PostGres extract and saves matches to linked_caserefs_master.csv
            matchAndSaveCaseRefsMaster();

            //Checks matches against extract from UnlinkedCaseReceipt table. Matches go to PaperReceipt file .csv,
            // others go back into linked_caserefs_master.csv.
            matchAndSaveUnlinkedCaseRefs();

            System.exit(0);

        } catch (SftpException | JSchException | IOException e) {
            System.out.println("Something went wrong...: " + e.getMessage());
        }

    }

    private static void importFileNamesAndLocationsFromPropertyFile() {

        Properties properties = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");

            properties.load(input);

            fileInputLocation = properties.getProperty("fileInputLocation");
            fileOutputLocationPaperReceipt = properties.getProperty("fileOutputLocationPaperReceipt");
            fileOutputLocationLinkedCaseRefsMaster = properties.getProperty("fileOutputLocationLinkedCaseRefsMaster");
            fileOutputLocationLocal = properties.getProperty("fileOutputLocationLocal");

            //Input files
            fileNameCaseRefsToCheck = properties.getProperty("fileNameCaseRefsToCheck");
            fileNameCaseRefExtract = properties.getProperty("fileNameCaseRefExtract");
            fileNameUnlinkedCaseReceiptExtract = properties.getProperty("fileNameUnlinkedCaseReceiptExtract");
            fileNameDrsReport = properties.getProperty("fileNameDrsReport");
            fileNameLinkedCaseRefsMaster = properties.getProperty("fileNameLinkedCaseRefsMaster");

            sftpHost = properties.getProperty("sftpHost");
            sftpUsername = properties.getProperty("sftpUsername");
            sftpPassword = properties.getProperty("sftpPassword");

        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(0);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static void createCaseRefsFromExtractToCheck() throws IOException, JSchException, SftpException {

        //Creates a 'master' list of caserefs from the CaseRef Extract to take into account any additional cases added since the last extract.
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);

        InputStream inputCaseRefsExtract = sftp.get(fileInputLocation + fileNameCaseRefExtract);
        System.out.println("Input Stream created for " + fileNameCaseRefExtract);
        HashMap<String, String> caseRefsExtractHash = createCaseRefsHashMap(inputCaseRefsExtract);
        renameCaseRefExtractFiles(sftp);

        //Set up local file location for linked_caserefs_master.csv
        String fileLocationCaseRefsToCheck = fileOutputLocationLocal + fileNameCaseRefsToCheck;
        System.out.println("Started writing to " + fileLocationCaseRefsToCheck);
        FileWriter fileWriterCaseRefsToCheck = new FileWriter(fileLocationCaseRefsToCheck, true);
        BufferedWriter bufferedWriterCaseRefsToCheck = new BufferedWriter(fileWriterCaseRefsToCheck);

        InputStream inputCaseRefsToCheck;

        try {
            inputCaseRefsToCheck = sftp.get(fileInputLocation + fileNameCaseRefsToCheck);
            System.out.println("Input Stream created for " + fileNameCaseRefExtract);

        } catch (SftpException e) {
            System.out.println("CaseRefsToCheck Error " + e.getLocalizedMessage());
            inputCaseRefsToCheck = null;
        }

        HashMap<String, String> caseRefsToCheckHash = new HashMap<>();
        if (inputCaseRefsToCheck != null) {
            caseRefsToCheckHash = createCaseRefsHashMap(inputCaseRefsToCheck);
        }

        for (Map.Entry<String,String> item : caseRefsExtractHash.entrySet()) {

            if (!caseRefsToCheckHash.containsKey(item.getKey())) {

                String actionId = item.getKey();
                String caseRef = item.getValue();

                System.out.println(actionId + "," + caseRef);

                //Write questionnaireId(unlinked caseref) and caseref to new Linked_caserefs csv
                bufferedWriterCaseRefsToCheck.write(actionId + "," + caseRef);
                bufferedWriterCaseRefsToCheck.write(System.getProperty("line.separator"));
            }

        }

        bufferedWriterCaseRefsToCheck.close();
        fileWriterCaseRefsToCheck.close();

        System.out.println("Finished writing to " + fileLocationCaseRefsToCheck);

        Session sessionUploadCaseRefsToCheck = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpUploadCaseRefsToCheck = getSftp(sessionUploadCaseRefsToCheck, fileOutputLocationLinkedCaseRefsMaster);

            //If caserefs_to_check.csv is successfully retrieved from sftp, delete it so updated version can be added after.
            System.out.println("Deleting old version of caserefs_to_check.csv");
            try {
                sftp.rm(fileOutputLocationLinkedCaseRefsMaster + fileNameCaseRefsToCheck);
                System.out.println(String.format("Deleted %s from SFTP", fileOutputLocationLinkedCaseRefsMaster + fileNameCaseRefsToCheck));
            } catch (SftpException e) {
                System.out.println(String.format("Error Deleting %s from SFTP: %s", fileOutputLocationLinkedCaseRefsMaster + fileNameCaseRefsToCheck, e.getLocalizedMessage()));
            }

        FileInputStream fileInputStreamCaseRefsToCheck = new FileInputStream(new File(fileLocationCaseRefsToCheck));
        try {
            sftpUploadCaseRefsToCheck.put(fileInputStreamCaseRefsToCheck, fileNameCaseRefsToCheck, ChannelSftp.OVERWRITE);
            System.out.println(String.format("Uploaded %s to SFTP", fileOutputLocationLinkedCaseRefsMaster));
        } catch (SftpException e) {
            System.out.println(String.format("Error uploading %s to SFTP: %s", fileOutputLocationLinkedCaseRefsMaster, e.getLocalizedMessage()));
        }

    }

    private static void matchAndSaveCaseRefsMaster() throws IOException, JSchException, SftpException {
        //Set up Sessions and Sftp Connections
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);
        Session sessionDrsReport = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpDrsReport = getSftp(sessionDrsReport, fileInputLocation);

        //Create HashMap of ActionIds + CaseRefs from Case Refs Extract
        InputStream inputCaseRefsExtract = sftp.get(fileInputLocation + fileNameCaseRefsToCheck);
        System.out.println("Input Stream created for " + fileNameCaseRefsToCheck);
        HashMap<String, String> caseRefsHash = createCaseRefsHashMap(inputCaseRefsExtract);

        //Set up local file location for linked_caserefs_master.csv
        String fileLocationLocalLinkedCaseRefsMaster = fileOutputLocationLocal + fileNameLinkedCaseRefsMaster;
        FileWriter fileWriterLinkedCaseRefsMaster = new FileWriter(fileLocationLocalLinkedCaseRefsMaster, true);
        BufferedWriter bufferedWriterLinkedCaseRefsMaster = new BufferedWriter(fileWriterLinkedCaseRefsMaster);

        System.out.println("Started writing to " + fileLocationLocalLinkedCaseRefsMaster);

        InputStream inputDrsReport = sftpDrsReport.get(fileInputLocation + fileNameDrsReport);
        System.out.println("Input Stream created for " + fileNameDrsReport);
        CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(inputDrsReport, "UTF-8")));
        System.out.println("CSVReader created");
        String [] nextLine;
        while ((nextLine = reader.readNext()) != null) {

            String dateOfVisit = parseDateOfVisit(nextLine[2]);
            String actionId = nextLine[0];
            String questionnaireId = nextLine[7];

            if(caseRefsHash.containsKey(actionId)) {

                System.out.println(questionnaireId + "," + caseRefsHash.get(actionId) + "," + dateOfVisit);

                //Write questionnaireId(unlinked caseref) and caseref to new Linked_caserefs csv
                bufferedWriterLinkedCaseRefsMaster.write(questionnaireId + "," + caseRefsHash.get(actionId) + "," + dateOfVisit);
                bufferedWriterLinkedCaseRefsMaster.write(System.getProperty("line.separator"));

            }

        }

        bufferedWriterLinkedCaseRefsMaster.close();
        fileWriterLinkedCaseRefsMaster.close();

        System.out.println("Finished writing to " + fileLocationLocalLinkedCaseRefsMaster);

        System.out.println("Started uploading to " + fileOutputLocationLinkedCaseRefsMaster);

        Session sessionUploadLinkedCaseRefsMaster = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpLinkedCaseRefsMaster = getSftp(sessionUploadLinkedCaseRefsMaster, fileOutputLocationLinkedCaseRefsMaster);

        System.out.println("Deleting old version of linked_case_refs_master.csv");
        try {
            sftpLinkedCaseRefsMaster.rm(fileOutputLocationLinkedCaseRefsMaster + fileNameLinkedCaseRefsMaster);
            System.out.println(String.format("Deleted %s from SFTP", fileOutputLocationLinkedCaseRefsMaster + fileNameLinkedCaseRefsMaster));
        } catch (SftpException e) {
            System.out.println(String.format("Error Deleting %s from SFTP: %s", fileOutputLocationLinkedCaseRefsMaster + fileNameLinkedCaseRefsMaster, e.getLocalizedMessage()));
        }

        FileInputStream fileInputStreamLinkedCaseRefsMaster = new FileInputStream(new File(fileLocationLocalLinkedCaseRefsMaster));
        try {
            sftpLinkedCaseRefsMaster.put(fileInputStreamLinkedCaseRefsMaster, fileNameLinkedCaseRefsMaster);
            System.out.println(String.format("Uploaded %s to SFTP", fileOutputLocationLinkedCaseRefsMaster));
        } catch (SftpException e) {
            System.out.println(String.format("Error uploading %s to SFTP: %s", fileOutputLocationLinkedCaseRefsMaster, e.getLocalizedMessage()));
        }

        renameDRSFiles(sftpDrsReport);

        disconnectSession(session, sftp);
        disconnectSession(sessionDrsReport, sftpDrsReport);
        disconnectSession(sessionUploadLinkedCaseRefsMaster, sftpLinkedCaseRefsMaster);

    }

    private static void matchAndSaveUnlinkedCaseRefs() throws SftpException, IOException, JSchException {

        //Set up Sessions and Sftp Connections
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);

        //Create HashMap of UnlinkedCaseRefs from Unlinked Case Receipt Extract
        List<String> unlinkedCaseRefsExtractList = createUnlinkedCaseReceiptList(sftp);

        //Set up file location for new paper receipt csv to write to
        String fileNamePaperReceipt = "Receipts_Generated_" + generatedDatestamp() + ".csv";
        String fileLocationPaperReceipt = fileOutputLocationLocal + fileNamePaperReceipt;
        FileWriter fileWriterPrintReceipt = new FileWriter(fileLocationPaperReceipt, false);
        BufferedWriter bufferedWriterPrintReceipt = new BufferedWriter(fileWriterPrintReceipt);

        //Set up file location for linked_caserefs_master to write to
        String fileLocationLocalCaseRefsMaster = fileOutputLocationLocal + fileNameLinkedCaseRefsMaster;
        FileWriter fileWriterLocalCaseRefsMaster = new FileWriter(fileLocationLocalCaseRefsMaster, false);
        BufferedWriter bufferedWriterCaseRefsMaster = new BufferedWriter(fileWriterLocalCaseRefsMaster);

        System.out.println("Started writing to " + fileLocationPaperReceipt);
        System.out.println("Started writing to " + fileLocationLocalCaseRefsMaster);

        //Set up file location for linked_caserefs_master
        System.out.println(fileInputLocation + fileNameLinkedCaseRefsMaster);
        InputStream inputLinkedCaseRefs = sftp.get(fileInputLocation + fileNameLinkedCaseRefsMaster);
        CSVReader linkedCaseRefsReader = new CSVReader(new BufferedReader(new InputStreamReader(inputLinkedCaseRefs, "UTF-8")));
        System.out.println("linkedCaseRefsReader:" + linkedCaseRefsReader.toString());
        String [] linkedCaseRefsNextLine;
        while ((linkedCaseRefsNextLine = linkedCaseRefsReader.readNext()) != null) {

            String unlinkedCaseRef = linkedCaseRefsNextLine[0];
            String caseRef = linkedCaseRefsNextLine[1];
            String dateOfVisit = linkedCaseRefsNextLine[2];

            if (unlinkedCaseRefsExtractList.contains(unlinkedCaseRef)) {
                //Write dateOfVisit and caseRef to new PrintReceipt.csv
                System.out.println("UnlinkedCaseRef matched: " + unlinkedCaseRef);
                bufferedWriterPrintReceipt.write(dateOfVisit + "," + caseRef);
                bufferedWriterPrintReceipt.write(System.getProperty("line.separator"));
            } else {
                //Write to new caserefs master csv
                System.out.println("UnlinkedCaseRef not matched: " + unlinkedCaseRef);
                bufferedWriterCaseRefsMaster.write(unlinkedCaseRef + "," + caseRef + "," + dateOfVisit);
                bufferedWriterCaseRefsMaster.write(System.getProperty("line.separator"));
            }

        }

        bufferedWriterPrintReceipt.close();
        fileWriterPrintReceipt.close();

        bufferedWriterCaseRefsMaster.close();
        fileWriterLocalCaseRefsMaster.close();

        System.out.println("Finished writing to " + fileLocationPaperReceipt);
        System.out.println("Finished writing to " + fileLocationLocalCaseRefsMaster);

        System.out.println("Started uploading to " + fileLocationPaperReceipt);

        Session sessionUploadPaperReceipt = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpPaperReceipt = getSftp(sessionUploadPaperReceipt, fileOutputLocationPaperReceipt);
        FileInputStream fileInputStreamPaperReceipt = new FileInputStream(new File(fileLocationPaperReceipt));

        try {
            sftpPaperReceipt.put(fileInputStreamPaperReceipt, fileNamePaperReceipt);
            System.out.println(String.format("Uploaded %s to SFTP", fileLocationPaperReceipt));
        } catch (SftpException e) {
            System.out.println(String.format("Error uploading %s to SFTP: %s", fileLocationPaperReceipt, e.getLocalizedMessage()));
        }

        System.out.println("Started uploading to " + fileLocationLocalCaseRefsMaster);

        Session sessionUploadCaseRefsMaster = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpCaseRefsMaster = getSftp(sessionUploadCaseRefsMaster, fileOutputLocationLinkedCaseRefsMaster);

        try {
            sftpCaseRefsMaster.rm(fileOutputLocationLinkedCaseRefsMaster + fileNameLinkedCaseRefsMaster);
            System.out.println(String.format("Deleted %s from SFTP", fileOutputLocationLinkedCaseRefsMaster));
        } catch (SftpException e) {
            System.out.println(String.format("Error Deleting %s from SFTP: %s", fileOutputLocationLinkedCaseRefsMaster, e.getLocalizedMessage()));
        }

        FileInputStream fileInputStreamCaseRefsMaster = new FileInputStream(new File(fileLocationLocalCaseRefsMaster));
        try {
            sftpCaseRefsMaster.put(fileInputStreamCaseRefsMaster, fileNameLinkedCaseRefsMaster);
            System.out.println(String.format("Uploaded %s to SFTP", fileLocationLocalCaseRefsMaster));
        } catch (SftpException e) {
            System.out.println(String.format("Error uploading %s to SFTP: %s", fileLocationLocalCaseRefsMaster, e.getLocalizedMessage()));
        }

        renameUnlinkedCaseRefExtractFiles(sftp);

        disconnectSession(session, sftp);
        disconnectSession(sessionUploadPaperReceipt, sftpPaperReceipt);
        disconnectSession(sessionUploadCaseRefsMaster, sftpCaseRefsMaster);

    }

    /**
     * Create and connect SFTP channel
     * @param session a Jsch session to connect to remote host
     * @return sftp sftp channel to transfer paper receipts
     * @throws JSchException if error with SSH protocol
     * @throws SftpException if error with SFTP
     */
    private static ChannelSftp getSftp(Session session, String path) throws JSchException, SftpException {
        ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
        sftp.connect();
        sftp.cd(path);
        System.out.println("STFP Connected: " + sftp.isConnected());
        return sftp;
    }

    /**
     * Create and connect SFTP channel
     * @return session connection session to remote host
     * @throws JSchException if error with SSH protocol
     */
    private static Session getSession(String username, String password) throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession( username, sftpHost, 22);
        session.setPassword(password);
        JSch.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        System.out.println("Session Connected " + session.isConnected());
        return session;
    }

    /**
     * Disconnect sftp channel and session
     * @param session session to disconnect
     * @param sftp sftp to disconnect
     */
    private static void disconnectSession(Session session, ChannelSftp sftp) {
        sftp.disconnect();
        session.disconnect();
        System.out.println("Disconnection Successful");
    }

    private static HashMap<String, String> createCaseRefsHashMap(InputStream caseRefsFile) throws IOException {
        System.out.println("Creating CaseRefs HashMap");
        CSVReader reader = new CSVReader(new InputStreamReader(caseRefsFile));
        String [] nextLine;
        HashMap<String, String> actionIdArray = new HashMap<>();
        while ((nextLine = reader.readNext()) != null) {
            actionIdArray.put(nextLine[0], nextLine[1]);
        }

        System.out.println("CaseRefs HashMap created");
        return actionIdArray;

    }

    private static String parseDateOfVisit(String dateOfVisit) {

        DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm", Locale.ENGLISH);

        try {
            Date dateOfVisitDate =  df.parse(dateOfVisit);
            DateFormat dfn = new SimpleDateFormat("dd/MM/yyyy", Locale.ENGLISH);

            dateOfVisit = dfn.format(dateOfVisitDate);

        } catch (ParseException e) {
            System.out.println("Date Parse Error" + e.getLocalizedMessage());
        }

        return dateOfVisit;
    }

    private static List<String> createUnlinkedCaseReceiptList(ChannelSftp sftp) throws IOException, SftpException {

        InputStream inputUnlinkedCaseReceiptExtract = sftp.get(fileInputLocation + fileNameUnlinkedCaseReceiptExtract);

        CSVReader reader = new CSVReader(new InputStreamReader(inputUnlinkedCaseReceiptExtract));
        String [] nextLine;
        List<String> unlinkedCaseRefList = new ArrayList<>();
        while ((nextLine = reader.readNext()) != null) {
            unlinkedCaseRefList.add(nextLine[0]);
            System.out.println(nextLine[0]);
        }

        return unlinkedCaseRefList;

    }

    private static String generatedDatestamp() {
        //Create ddMMyyyy datestamp for generated file names
        LocalDate localDate = LocalDate.now();
        DateTimeFormatter localDateFormat = DateTimeFormatter.ofPattern("ddMMyyyy");
        return localDateFormat.format(localDate);
    }

    @SuppressWarnings("unchecked")
    private static void renameDRSFiles(ChannelSftp sftp) throws SftpException {

        Vector<LsEntry> fileList = sftp.ls(fileInputLocation + fileNameDrsReport);

        for (LsEntry lsEntry : fileList) {
            String file = lsEntry.getFilename();
            String newPath = file + ".processed";

            System.out.println("Now processing file " + file);
            sftp.rename(file, newPath);
            System.out.println("File renamed to " + newPath);

        }

    }

    @SuppressWarnings("unchecked")
    private static void renameUnlinkedCaseRefExtractFiles(ChannelSftp sftp) throws SftpException {

        Vector<LsEntry> fileList = sftp.ls(fileInputLocation + fileNameUnlinkedCaseReceiptExtract);

        for (LsEntry lsEntry : fileList) {
            String file = lsEntry.getFilename();
            String newPath = file + ".processed";

            System.out.println("Now processing file " + file);
            sftp.rename(file, newPath);
            System.out.println("File renamed to " + newPath);

        }

    }

    @SuppressWarnings("unchecked")
    private static void renameCaseRefExtractFiles(ChannelSftp sftp) throws SftpException {

        Vector<LsEntry> fileList = sftp.ls(fileInputLocation + fileNameCaseRefExtract);

        for (LsEntry lsEntry : fileList) {
            String file = lsEntry.getFilename();
            String newPath = file + ".processed";

            System.out.println("Now processing file " + file);
            sftp.rename(file, newPath);
            System.out.println("File renamed to " + newPath);

        }

    }

}
