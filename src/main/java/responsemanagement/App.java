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
    private static String fileOutputLocationLinkedCaseRefsAll;
    private static String fileOutputLocationScannedNotMatched;
    private static String fileOutputLocationLocal;

    //File Names
    private static String fileNameCaseRefsToCheck;
    private static String fileNameCaseRefExtract;
    private static String fileNameUnlinkedCaseReceiptExtract;
    private static String fileNameDrsReport;
    private static String fileNameLinkedCaseRefsMaster;
    private static String fileNameLinkedCaseRefsAll;
    private static String fileNameScannedNotMatched;

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
            fileOutputLocationLinkedCaseRefsAll = properties.getProperty("fileOutputLocationLinkedCaseRefsAll");
            fileOutputLocationScannedNotMatched = properties.getProperty("fileOutputLocationScannedNotMatched");
            fileOutputLocationLocal = properties.getProperty("fileOutputLocationLocal");

            //Input files
            fileNameCaseRefsToCheck = properties.getProperty("fileNameCaseRefsToCheck");
            fileNameCaseRefExtract = properties.getProperty("fileNameCaseRefExtract");
            fileNameUnlinkedCaseReceiptExtract = properties.getProperty("fileNameUnlinkedCaseReceiptExtract");
            fileNameDrsReport = properties.getProperty("fileNameDrsReport");
            fileNameLinkedCaseRefsMaster = properties.getProperty("fileNameLinkedCaseRefsMaster");
            fileNameLinkedCaseRefsAll = properties.getProperty("fileNameLinkedCaseRefsAll");
            fileNameScannedNotMatched = properties.getProperty("fileNameScannedNotMatched");

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

        //Creates a 'master' list of caserefs from the CaseRef Extract to take into account
        // any additional cases added since the last extract.
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

        disconnectSession(sessionUploadCaseRefsToCheck, sftpUploadCaseRefsToCheck);
        disconnectSession(session, sftp);

    }

    private static void matchAndSaveCaseRefsMaster() throws IOException, JSchException, SftpException {
        //Set up Sessions and Sftp Connections
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);

        //Create HashMap of ActionIds + CaseRefs from Case Refs Extract
        InputStream inputCaseRefsExtract = sftp.get(fileInputLocation + fileNameCaseRefsToCheck);
        System.out.println("Input Stream created for " + fileNameCaseRefsToCheck);
        disconnectSession(session, sftp);
        HashMap<String, String> caseRefsHash = createCaseRefsHashMap(inputCaseRefsExtract);

        //Set up local file location for linked_caserefs_master.csv
        String fileLocationLocalLinkedCaseRefsMaster = fileOutputLocationLocal + fileNameLinkedCaseRefsMaster;
        FileWriter fileWriterLinkedCaseRefsMaster = new FileWriter(fileLocationLocalLinkedCaseRefsMaster, true);
        BufferedWriter bufferedWriterLinkedCaseRefsMaster = new BufferedWriter(fileWriterLinkedCaseRefsMaster);

        System.out.println("Started writing to " + fileLocationLocalLinkedCaseRefsMaster);

        HashMap<String, String> linkedCaseRefsHash = new HashMap<>();

        InputStream inputDrsReport = sftp.get(fileInputLocation + fileNameDrsReport);
        System.out.println("Input Stream created for " + fileNameDrsReport);
        CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(inputDrsReport, "UTF-8")));
        System.out.println("CSVReader created");
        String [] nextLine;
        while ((nextLine = reader.readNext()) != null) {

            String dateOfVisit = parseDateOfVisit(nextLine[2]);
            String actionId = nextLine[0];
            String questionnaireId = nextLine[7];

            if(caseRefsHash.containsKey(actionId)) {

                //Write questionnaireId(unlinked caseref) and caseref to new Linked_caserefs csv
                bufferedWriterLinkedCaseRefsMaster.write(questionnaireId + "," + caseRefsHash.get(actionId) + "," + dateOfVisit);
                bufferedWriterLinkedCaseRefsMaster.write(System.getProperty("line.separator"));
                //System.out.println(questionnaireId + "," + caseRefsHash.get(actionId) + "," + dateOfVisit);

                linkedCaseRefsHash.put(questionnaireId, caseRefsHash.get(actionId));

            }

        }

        writeToLinkedCaseRefsAll(linkedCaseRefsHash);

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

        renameDRSFiles(sftp);

        disconnectSession(session, sftp);
        disconnectSession(sessionUploadLinkedCaseRefsMaster, sftpLinkedCaseRefsMaster);

    }

    private static void writeToLinkedCaseRefsAll(HashMap<String, String> linkedCaseRefsHash) throws SftpException, IOException, JSchException {

        //Set up Sessions and Sftp Connections
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);
        //CSV input for existing linked_caserefs_all and create HashMap from this
        HashMap<String, String> caseRefsAllHashMap = createLinkedCaseRefsAllHashMap(sftp);
        disconnectSession(session, sftp);

        //Set up local file location for linked_caserefs_master.csv
        String fileLocationLocalLinkedCaseRefsAll = fileOutputLocationLocal + fileNameLinkedCaseRefsAll;
        FileWriter fileWriterLinkedCaseRefsAll = new FileWriter(fileLocationLocalLinkedCaseRefsAll, true);
        BufferedWriter bufferedWriterLinkedCaseRefsAll = new BufferedWriter(fileWriterLinkedCaseRefsAll);

        for (Map.Entry<String,String> item : linkedCaseRefsHash.entrySet()) {

            String questionnaireId = item.getKey();
            String caseRef = item.getValue();

            if (!caseRefsAllHashMap.containsKey(item.getKey())) {

                //Write questionnaireId(unlinked caseref) and caseref linked_caserefs_all.csv
                bufferedWriterLinkedCaseRefsAll.write(questionnaireId + "," + caseRef);
                bufferedWriterLinkedCaseRefsAll.write(System.getProperty("line.separator"));

                System.out.println(questionnaireId + "," + caseRef + " added to linked_caserefs_all.csv");
            } else {
                System.out.println(questionnaireId + "," + caseRef + "already exists in linked_caserefs_all.csv");
            }

        }

        bufferedWriterLinkedCaseRefsAll.close();
        fileWriterLinkedCaseRefsAll.close();

        System.out.println("Finished writing to " + fileLocationLocalLinkedCaseRefsAll);

        System.out.println("Started uploading to " + fileLocationLocalLinkedCaseRefsAll);

        Session sessionUploadScannedLinkedCaseRefsAll = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpLinkedCaseRefsAll = getSftp(sessionUploadScannedLinkedCaseRefsAll, fileOutputLocationLinkedCaseRefsAll);

        try {
            sftpLinkedCaseRefsAll.rm(fileOutputLocationLinkedCaseRefsAll + fileNameLinkedCaseRefsAll);
            System.out.println(String.format("Deleted %s from SFTP", fileOutputLocationLinkedCaseRefsAll));
        } catch (SftpException e) {
            System.out.println(String.format("Error Deleting %s from SFTP: %s", fileOutputLocationLinkedCaseRefsAll, e.getLocalizedMessage()));
        }

        FileInputStream fileInputStreamLinkedCaseRefsAll = new FileInputStream(new File(fileLocationLocalLinkedCaseRefsAll));
        try {
            sftpLinkedCaseRefsAll.put(fileInputStreamLinkedCaseRefsAll, fileNameLinkedCaseRefsAll);
            System.out.println(String.format("Uploaded %s to SFTP", fileLocationLocalLinkedCaseRefsAll));
        } catch (SftpException e) {
            System.out.println(String.format("Error uploading %s to SFTP: %s", fileLocationLocalLinkedCaseRefsAll, e.getLocalizedMessage()));
        }

        disconnectSession(sessionUploadScannedLinkedCaseRefsAll, sftpLinkedCaseRefsAll);

    }

    private static void matchAndSaveUnlinkedCaseRefs() throws SftpException, IOException, JSchException {

        //Set up Sessions and Sftp Connections
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);

        //Create List of UnlinkedCaseRefs from Unlinked Case Receipt Extract
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
                //System.out.println("UnlinkedCaseRef matched: " + unlinkedCaseRef);
                bufferedWriterPrintReceipt.write(dateOfVisit + "," + caseRef);
                bufferedWriterPrintReceipt.write(System.getProperty("line.separator"));

                //If matched, remove caseref from UnlinkedExtract List so can be added to scanned_not_matched.csv
                unlinkedCaseRefsExtractList.remove(unlinkedCaseRef);

            } else {
                //Write to new caserefs master csv
                //System.out.println("UnlinkedCaseRef not matched: " + unlinkedCaseRef);
                bufferedWriterCaseRefsMaster.write(unlinkedCaseRef + "," + caseRef + "," + dateOfVisit);
                bufferedWriterCaseRefsMaster.write(System.getProperty("line.separator"));

            }

        }

        //Write remaining (not matched) UnlinkedCaseRefs from Extract to scanned_not_matched.csv
        writeToScannedNotMatched(unlinkedCaseRefsExtractList);

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

    private static void writeToScannedNotMatched(List<String> unlinkedCaseRefsExtractList) throws IOException, JSchException, SftpException {

        //Set up Sessions and Sftp Connections
        Session session = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftp = getSftp(session, fileInputLocation);

        //CSV input for existing scanned_not_matched and create List from this
        List<String> scannedNotMatchedList = createScannedNotMatchedList(sftp);
        disconnectSession(session, sftp);

        //Set up file location for linked_caserefs_master to write to
        String fileLocationLocalScannedNotMatched = fileOutputLocationLocal + fileNameScannedNotMatched;
        FileWriter fileWriterScannedNotMatched = new FileWriter(fileLocationLocalScannedNotMatched, true);
        BufferedWriter bufferedWriterScannedNotMatched = new BufferedWriter(fileWriterScannedNotMatched);

        System.out.println("Started writing to " + fileLocationLocalScannedNotMatched);

        //For each item in unlinkedCaseRefsExtractList check if in scanned_not_matched
        //if not in scanned_not_matched, add to csv
        for (String unlinkedCaseRef : unlinkedCaseRefsExtractList) {

            if (!scannedNotMatchedList.contains(unlinkedCaseRef)) {
                System.out.println(unlinkedCaseRef + " added to scanned_not_matched");
                bufferedWriterScannedNotMatched.write(unlinkedCaseRef);
                bufferedWriterScannedNotMatched.write(System.getProperty("line.separator"));
            } else {
                System.out.println(unlinkedCaseRef + " already exists in scanned_not_matched");
            }

        }

        bufferedWriterScannedNotMatched.close();
        fileWriterScannedNotMatched.close();

        System.out.println("Finished writing to " + fileLocationLocalScannedNotMatched);

        System.out.println("Started uploading to " + fileLocationLocalScannedNotMatched);

        Session sessionUploadScannedNotMatched = getSession(sftpUsername, sftpPassword);
        ChannelSftp sftpScannedNotMatched = getSftp(sessionUploadScannedNotMatched, fileOutputLocationScannedNotMatched);

        try {
            sftpScannedNotMatched.rm(fileOutputLocationScannedNotMatched + fileNameScannedNotMatched);
            System.out.println(String.format("Deleted %s from SFTP", fileOutputLocationScannedNotMatched));
        } catch (SftpException e) {
            System.out.println(String.format("Error Deleting %s from SFTP: %s", fileOutputLocationScannedNotMatched, e.getLocalizedMessage()));
        }

        FileInputStream fileInputStreamScannedNotMatched = new FileInputStream(new File(fileLocationLocalScannedNotMatched));
        try {
            sftpScannedNotMatched.put(fileInputStreamScannedNotMatched, fileNameScannedNotMatched);
            System.out.println(String.format("Uploaded %s to SFTP", fileLocationLocalScannedNotMatched));
        } catch (SftpException e) {
            System.out.println(String.format("Error uploading %s to SFTP: %s", fileLocationLocalScannedNotMatched, e.getLocalizedMessage()));
        }

        disconnectSession(sessionUploadScannedNotMatched, sftpScannedNotMatched);

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

    private static HashMap<String, String> createLinkedCaseRefsAllHashMap(ChannelSftp sftp) throws IOException, SftpException {

        InputStream inputLinkedCaseRefsAll;

        try {
            inputLinkedCaseRefsAll = sftp.get(fileInputLocation + fileNameLinkedCaseRefsAll);
            System.out.println("Input Stream created for " + fileNameLinkedCaseRefsAll);

        } catch (SftpException e) {
            System.out.println("LinkedCaseRefsAll Error " + e.getLocalizedMessage());
            return new HashMap<>();
        }

        CSVReader reader = new CSVReader(new InputStreamReader(inputLinkedCaseRefsAll));
        String [] nextLine;
        HashMap<String, String> linkedCaseRefsAllHash = new HashMap<>();
        while ((nextLine = reader.readNext()) != null) {
            linkedCaseRefsAllHash.put(nextLine[0], nextLine[1]);
            System.out.println(nextLine[0] + ", " + nextLine[1]);
        }

        System.out.println("LinkedCaseRefsAll HashMap created");
        return linkedCaseRefsAllHash;

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

    private static List<String> createScannedNotMatchedList(ChannelSftp sftp) throws IOException, SftpException {

        InputStream inputScannedNotMatched;

        try {
            inputScannedNotMatched = sftp.get(fileInputLocation + fileNameScannedNotMatched);
            System.out.println("Input Stream created for " + fileNameScannedNotMatched);

        } catch (SftpException e) {
            System.out.println("ScannedNotMatched Error " + e.getLocalizedMessage());
            return new LinkedList<>();
        }

        CSVReader reader = new CSVReader(new InputStreamReader(inputScannedNotMatched));
        String [] nextLine;
        List<String> scannedNotMatchedList = new ArrayList<>();
        while ((nextLine = reader.readNext()) != null) {
            scannedNotMatchedList.add(nextLine[0]);
            System.out.println(nextLine[0]);
        }

        return scannedNotMatchedList;

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
