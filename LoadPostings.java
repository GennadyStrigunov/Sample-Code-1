package rb.bars.etl.posts;

import lv.gcpartners.bank.ProcessRB;
import lv.gcpartners.bank.ConnectionFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import lv.gcpartners.bank.Constants;
import rb.bars.core.utils.WorkWithTmpPDLOAD;
import rb.bars.core.utils.GenerateWhereSQLForParts;
import java.sql.SQLException;
import java.util.Vector;

/**
 * <p>Title: �������� ������ � ����. </p>
 * <p>Description: �������� �������� �� EODPOPD � PDLOAD. </p>
 * <p>Copyright: Copyright (c) 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014. </p>
 * <p>Company: RB </p>
 * @author ��������� �.�.
 * @version 1.0
 */

public class LoadPostings extends ProcessRB {

  /**
   * ����������� ������. <BR>
   */
  public LoadPostings() {
    super();
    // set working schema
    schemaDWHIN = ConnectionFactory.getFactory().getOption("schema_dwhin");
    schemaDWH   = ConnectionFactory.getFactory().getOption("schema_dwh");
    PDLOAD      = ConnectionFactory.getFactory().getOption("pdload");
    GCPEVENTS   = ConnectionFactory.getFactory().getOption("gcpevents");
  }

  /**
   * Start load process. <BR>
   * @param schTName ��� ����� � ������� PDLOAD ���� ��������� ������������; <BR>
   * @param eodpopdTmp �����+������������ ��������� ������� EODPOPD, ������ �������� ������ � PDLOAD; <BR>
   * @param eodpopdSrc �����+������������ �������� ������� EODPOPD, ������ �������� ������ MIDAS; <BR>
   * @param connForSave java.sql.Connection ��� ������ ������ � PDLOAD; <BR>
   * @param pod ����, �� ������� ��������� ������ �� MIDAS
   * @param eodpodeals ��������� ������� ������ ��� �������� � PDLOAD; <BR>
   * @param accntabTmp ��������� ������� ������ MIDAS; <BR>
   * @param pbrs ������ ������� ��� ����������� ����� �������; <BR>
   * @param isFillEodpopd true - ��������� ��������� ������� EODPOPD, false - �� ���������; <BR>
   * @param isFillDeals true - ��������� �������� ������� EODPODEALS, false - �� ���������; <BR>
   * @param isInsFromPDLOAD true - �������� �� ��������� ������� ������������ � PDLOAD ��������, false - �� ���������; <BR>
   * @param isDelFromPDLOAD true - ������� ������������ � PDLOAD �������� �� ��������� ����, false - �� �������; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  public void process(String schTName, String eodpopdTmp, String eodpopdSrc, java.sql.Connection connForSave, java.sql.Date pod,
    String eodpodeals, String accntabTmp, String pbrs, boolean isFillEodpopd, boolean isFillDeals, boolean isInsFromPDLOAD,
    boolean isDelFromPDLOAD) throws Exception {

    logger.info("Start load Midas postings from " + eodpopdSrc + " using " + eodpopdTmp + "," + eodpodeals + "," + accntabTmp + " into " + schTName + " for " + pod);

    try {

      // ���������������� ���������
      initialize(connForSave);

      // ���������� ����� ������ ����
      curVer = Integer.parseInt(String.valueOf(this.getParam("BARSCodeVersion", pod, connection, schemaDWH)));

      // �������� RUDWH.EODPOPD(������) ---> RUBARS01.EODPOPD(RUBARS01.EODPODEALS) ---> RUBARS01.PDLOAD
      loadPostings(schTName, eodpopdTmp, eodpopdSrc, pod, eodpodeals, accntabTmp, pbrs, isFillEodpopd, isFillDeals,
          isInsFromPDLOAD, isDelFromPDLOAD);

      // ����������� ������ ���������� �� TMPP2BLST � PDLOADDOC, ���������� �������� � ���� AVISTP
      try {
        cpyP2BLST(connection, pod, schTName);
      } catch (Exception ex1) {
        logger.error("������ ��� ����������� ������ �� " + schemaDWH + ".TMPP2BLST � " + schemaDWH + ".PDLOADDOC ", ex1);
      }

      // �������� �� ���������� ����� fxsa,fxpa � ������������� (�� ������� dextab)
      checkDextabFXPA_FXSA(pod, schTName);

    } catch (Exception ex) {

      finishCode = -1;
      logger.error("Load Midas postings failed ", ex);
      connection.rollback();
      throw ex;

    } finally {

      if (connForSave == null)
        closeResource();
      logger.info("Finish Midas postings ");

    }
  }

  /**
   * ������������� ��������� � ��. <BR>
   * @param conn java.sql.Connection ��� ������ � ��; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void initialize(java.sql.Connection conn) throws Exception {
    if (conn == null) {
      connection = ConnectionFactory.getFactory().getConnection();
      connection.setAutoCommit(true);
      connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
    } else
      connection = conn;
  }

  /**
   * �������� ���������� ����� FXSA, FXPA �� ������� DEXTAB. <BR>
   * @param wrkDay ����, �� ������� ��������� ��������; <BR>
   * @param destForSave ����� � ��� ������� PDLOAD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void checkDextabFXPA_FXSA(java.sql.Date wrkDay, String destForSave) throws Exception {
    java.sql.Statement st = connection.createStatement();
    ResultSet rss = st.executeQuery(
      "select count(*) " +
      "from " + destForSave + " p, " + schemaDWH + ".dextab d " +
      "where '01.01.2029'=d.datto and p.dlid=dealid and p.pbr='GE-DL' and " +
      "p.pod='" + wrkDay.toString() + "' and (fxsa is null or fxpa is null) ");
    if (rss.next()) {
      if (rss.getInt(1) > 0)
        logger.info("���� FXSA, FXPA � " + destForSave + " �� " + wrkDay + " �� ������� DEXTAB ������, ���-�� " + rss.getInt(1));
    }
    rss.close();
    rss = null;
    st.close();
    st = null;
  }

  /**
   * ��������� �������� �������� � �������� ������� EODPOPD. ������ ��������� �������� ������ �� ���� ����. <BR>
   * @param eodpopdSrc �������� ������� MIDAS, ������ ������ ������ MIDAS; <BR>
   * @return �������� ���������: true - �������� � EODPOPD �� ���� ����, false - �������� � EODPOPD �� ������ ����; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private boolean checkPostingsInEodpopd(String eodpopdSrc) throws Exception {
    String sql2 = "select count(*), date(pstd+719892) from " + eodpopdSrc + " group by pstd ";
    PreparedStatement statement2 = connection.prepareStatement(sql2);
    ResultSet result2 = statement2.executeQuery();
    int cntDay = 0;
    long pdCount = 0;
    java.sql.Date pod = null;
    while (result2.next()) {
      cntDay++;
      pdCount = result2.getLong(1);
      pod = result2.getDate(2);
      logger.info(eodpopdSrc + ": dat=" + pod + " count=" + pdCount);
    }
    result2.close();
    if (cntDay != 1)
      return false;
    return true;
  }

  /**
   * ��������� �������� � PDLOAD - ���� �� ��� �� �������� ����. <BR>
   * @param schTName ����� + ������� PDLOAD; <BR>
   * @param pod ����, �� ������� ��������� �������� � PDLOAD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void checkPostingsInPDLOAD(String schTName, java.sql.Date pod) throws Exception {
    // proverka soderzimogo PDLOAD : BATCHID=3 - ������� �����, ����� ��� ������ � ����������� �����
    String sql = "select count(*) from " + schTName + " where pod=? and status='I' and batchid in (0,1) ";
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setDate(1, pod);
    ResultSet result = statement.executeQuery();
    long pdloadCount = 0;
    if (result.next())
      pdloadCount = result.getLong(1);
    result.close();
    if (pdloadCount > 0) {
      logger.error("� " + schTName + " ��� ���� ������������� �� " + pod.toString() + " � ���-�� =" + pdloadCount);
//      throw new Exception("� " + schTName + " ��� ���� ������������� �� " + pod.toString() + " � ���-�� =" + pdloadCount);
    }
  }

  /**
   * �������� ������ ������� ������, ��� ����������� � ����������� ����. <BR>
   * @param schTName ����� + ������� PDLOAD; <BR>
   * @param pod ���� ���������; <BR>
   * @return ������ ������, ������� ��� ����������� � PDLOAD; <BR>
   * @throws Exception ��������� �������. <BR>
   */
  private String getLoadedBatches(String schTName, java.sql.Date pod) throws Exception {
    java.sql.Statement st = connection.createStatement();
    ResultSet rss = st.executeQuery("select distinct substr(pbr,1,3) from " + schTName + " " +
      "where pod='" + pod.toString() + "' and batchid in (1, 3) AND STATUS='I' ");
    boolean isFirst = true;
    String pbrBatchNotForLoad = "";
    while (rss.next()) {
      pbrBatchNotForLoad = pbrBatchNotForLoad + (isFirst ? "" : ",") + "'" + rss.getString(1) + "'";
      isFirst = false;
    }
    rss.close();
    st.close();
    logger.info("������ �� ������ BATCH, ������� �� ������� � " + pod.toString() + ": " + pbrBatchNotForLoad);
    return pbrBatchNotForLoad;
  }

  /**
   * Zagruzka provodok iz EODPOPD. <BR>
   * @param schTName ��� ����� � ������� ���� ��������� ������������; <BR>
   * @param eodpopdTmp �����+������������ ��������� ������� EODPOPD ������ �������� ������ � PDLOAD; <BR>
   * @param eodpopdSrc �����+������������ �������� ������� EODPOPD (MIDAS); <BR>
   * @param wrkDay ����, �� ������� �������� ������ �� MIDAS; <BR>
   * @param eodpodealsTmp ��������� ������� ������ ������ ��� �������� � PDLOAD; <BR>
   * @param accntabTmp ��������� ������� ������ MIDAS; <BR>
   * @param pbrs ������ ������� ����� ������� ��� ����������� � PDLOAD; <BR>
   * @param isFillEODPOPD true - ��������� ��������� ������� EODPOPD, false - �� ���������; <BR>
   * @param isFillDeals true - ��������� �������� ������� EODPODEALS, false - �� ���������; <BR>
   * @param isInsFromPDLOAD true - �������� �� ��������� ������� ������������ � PDLOAD ��������, false - �� ���������; <BR>
   * @param isDelFromPDLOAD true - ������� ������������ � PDLOAD �������� �� ��������� ����, false - �� �������; <BR>
   * @return �������� � �������� ������ ������� (true) ��� ��� (false); <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private boolean loadPostings(String schTName, String eodpopdTmp, String eodpopdSrc, java.sql.Date wrkDay, String eodpodealsTmp,
    String accntabTmp, String pbrs, boolean isFillEODPOPD, boolean isFillDeals, boolean isInsFromPDLOAD, boolean isDelFromPDLOAD)
    throws Exception {

    /** ��������� �������� � EODPOPD - ������ ���� �� ���� ���� */
    if (!checkPostingsInEodpopd(eodpopdSrc))
      throw new Exception(eodpopdSrc + " contain incorrect postings");

    /** ��������� ���������� �������� � PDLOAD �� �������� I */
    checkPostingsInPDLOAD(schemaDWH + "." + PDLOAD, wrkDay);

    /** ��������� ������� EODPOPD ���������� */
    if (isFillEODPOPD && !fillEODPOPD(wrkDay, eodpopdTmp, eodpopdSrc))
      throw new Exception("������ ��� ���������� ������� " + eodpopdTmp);
    /** ��������� ������� EODPODEALS �� ������� ��� �������� �� � PDLOAD */
    if (isFillDeals && !gatherDeals(connection, wrkDay, eodpopdTmp, eodpodealsTmp))
      throw new Exception("������ ��� ���������� ������� " + eodpodealsTmp);
//    if (true)
//      return true;

    /** �������� ������ ������, ��� ����������� � PDLOAD */
    String pbrBatchNotForLoad = getLoadedBatches(schemaDWH + "." + PDLOAD, wrkDay);

    // �������� ������� <PDLOAD>, ����� �������� ��������, �������� ������� � �������������
    // ���� schTName=RUBARS01.PDLOAD, �� �� ������� ��������� �������
    WorkWithTmpPDLOAD wwtP = new WorkWithTmpPDLOAD();
    if (!schTName.equals(schemaDWH + "." + PDLOAD)) {
      // �������� �������
      wwtP.crtTable(schTName, schTName, wrkDay, isDelFromPDLOAD, "POD='" + wrkDay + "'", false, false, false, null,
          schemaDWH + "." + PDLOAD, true, isInsFromPDLOAD);
      // ��� ������
//      wwtP.crtTable(schTName, schTName, wrkDay, false, "POD='" + wrkDay + "'", false, false, false, null, schemaDWH + "." + PDLOAD, true, false);
    }

    /** ��������� ���������� ����������� �� RUBARS01.EODPOPD(RUBARS01.EODPODEALS) ---> RUBARS01.<PDLOAD> */
    // ��� ��������
    if (!startProcess(wrkDay, schTName, eodpopdTmp, eodpopdSrc, isInsFromPDLOAD ? pbrBatchNotForLoad : null, eodpodealsTmp, accntabTmp, pbrs))
    // ��� ������
//    if (!startProcess(wrkDay, schTName, eodpopdTmp, eodpopdSrc, null, eodpodealsTmp, accntabTmp, pbrs))
      throw new Exception("������ ��� ���������� ������� " + schTName);

    /** ������� ���-�� �������� � EODPOPD � � <PDLOAD> - ������ ���� ����� */
    checkLoadedPstsCnt(wrkDay, schTName, eodpopdSrc); // ������� � ������ ����������� ���� � ��������������� ������� - �� ����� ������������� ����������

    /** ������� ������� � ��������������� */
    // ���� schTName=RUBARS01.PDLOAD, �� �� ������� ������� � �� �����������
    if (!schTName.equals(schemaDWH + "." + PDLOAD)) {
      wwtP.crtIndexes(schTName, connection, true);
      //2014-10-14 ������
      if (true || schemaDWH.toUpperCase().startsWith("RU") || schemaDWH.toUpperCase().startsWith("T2"))
        wwtP.strBARSJrn(schTName, connection);
    }

    return true;
  }

  /**
   * ������� ���-�� �������� � ����������� ��������, � ������ �� ���������� ������ ������. <BR>
   * @param pod ����, �� ������� ��������� ��������; <BR>
   * @param schTName ��� ������� <PDLOAD> ������ �� ������; <BR>
   * @param eodpopdSrc ��� �������� ������� EODPOPD ������ �� ������; <BR>
   * @return true - ���-�� ���������, false - ���-�� �� ���������; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private boolean checkLoadedPstsCnt(java.sql.Date pod, String schTName, String eodpopdSrc) throws Exception {
    logger.info("��������� �� ����������� � ���-�� �������� (" + eodpopdSrc + ") � ����������� (" + schTName + ") �������� �� " + pod);
    boolean isOK = true;
    java.sql.Statement st = connection.createStatement();
    ResultSet rss = st.executeQuery(
      "WITH A (PBR, CNT) AS (" +
      "SELECT CASE WHEN LEFT(LTRIM(SPOS), 3)<>'GE-' THEN LEFT(SPOS, 3) ELSE LTRIM(SPOS) END, COUNT(*) FROM " + eodpopdSrc + " " +
      "WHERE (LENGTH(TRIM(SPOS))=6 AND LEFT(LTRIM(SPOS), 3)<>'GE-') OR LEFT(LTRIM(SPOS), 3)='GE-' " +
      "GROUP BY CASE WHEN LEFT(LTRIM(SPOS), 3)<>'GE-' THEN LEFT(SPOS, 3) ELSE LTRIM(SPOS) END" +
      "), B (PBR, CNT) AS (" +
      "SELECT CASE WHEN LEFT(PBR, 3)<>'GE-' THEN LEFT(PBR, 3) ELSE PBR END, COUNT(*) FROM " + schTName + " " +
      "WHERE STATUS='I' AND POD='" + pod + "' AND ((LENGTH(TRIM(PBR))=6 AND LEFT(PBR, 3)<>'GE-') OR LEFT(PBR, 3)='GE-') " +
      "GROUP BY CASE WHEN LEFT(PBR, 3)<>'GE-' THEN LEFT(PBR, 3) ELSE PBR END" +
      ") SELECT A.PBR, A.CNT, B.CNT FROM A A LEFT JOIN B B ON A.PBR=B.PBR WHERE B.PBR IS NULL OR A.CNT<>B.CNT " +
      "UNION SELECT B.PBR, A.CNT, B.CNT FROM A A RIGHT JOIN B B ON A.PBR=B.PBR WHERE A.PBR IS NULL " +
      "");
    while (rss.next()) {
      logger.info(rss.getString(1) + " : " + rss.getInt(2) + " - " + rss.getInt(3));
      isOK = false;
    }
    rss.close();
    st.close();
    return isOK;
  }

  /**
   * ��������� �������� ������ MIDAS EODPOPD �� ������������� ����� � RUBARS01. <BR>
   * @param pod ���� ����.���; <BR>
   * @param eodpopdTmp ��������� ������� EODPOPD, ��� ���������� ����������� ������ ����� ������������ � PDLOAD; <BR>
   * @param eodpopdSrc �������� ������� EODPOPD, ������ ����� ����������� ����������� �������� ������ MIDAS; <BR>
   * @return true - ������ EODPOPD �������������� �� RUDWH � RUBARS01, false - ������ EODPOPD �� �������������� �� RUDWH � RUBARS01; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  public boolean fillEODPOPD(java.sql.Date pod, String eodpopdTmp, String eodpopdSrc) throws Exception {
    java.sql.Statement st = connection.createStatement();
//    lv.gcpartners.bank.util.CallCommandOnAS400 prgAS400 = new lv.gcpartners.bank.util.CallCommandOnAS400();
//    String cmd1 = "CLRPFM FILE(" + eodpopdTmp.replace(".".charAt(0), "/".charAt(0)) + ") ";
//    if (!prgAS400.call(cmd1, null, null)) { // ����� ����������� �������� �������
//      throw new Exception ("������ ��� ������� �������� ���� " + eodpopdTmp + "");
//    }
//    logger.info("������� " + eodpopdTmp + " �������");

    try {
      st.execute("DROP TABLE " + eodpopdTmp);
      logger.info("������������ ������� " + eodpopdTmp + " �������");
    } catch (SQLException ex) {
    }
    try {
      st.execute("CREATE TABLE " + eodpopdTmp + " (CNUM CHAR(6) CCSID 1025 DEFAULT NULL, CCY CHAR(3) CCSID 1025 DEFAULT NULL, " +
        "ACOD CHAR(4) CCSID 1025 DEFAULT NULL, ACSQ CHAR(2) CCSID 1025 DEFAULT NULL, POD DATE DEFAULT NULL, VALD DATE DEFAULT NULL, " +
        "PNAR CHAR(30) CCSID 1025 DEFAULT NULL, PSTA DECIMAL(13, 0) DEFAULT NULL, DRCR DECIMAL(1, 0) DEFAULT NULL, " +
        "ASOC CHAR(6) CCSID 1025 DEFAULT NULL, SPOS CHAR(7) CCSID 1025 DEFAULT NULL, BRCA CHAR(3) CCSID 1025 DEFAULT NULL, " +
        "DPMT CHAR(3) CCSID 1025 DEFAULT NULL, VOIN DECIMAL(1, 0) DEFAULT NULL, DLREF CHAR(8) CCSID 1025 DEFAULT NULL, " +
        "OTRF CHAR(15) CCSID 1025 DEFAULT NULL, OTST CHAR(10) CCSID 1025 DEFAULT NULL, OTTP CHAR(10) CCSID 1025 DEFAULT NULL, " +
        "PBRN CHAR(7) CCSID 1025 DEFAULT NULL, BATCHPBR CHAR(3) CCSID 1025 DEFAULT NULL, DLNO CHAR(6) CCSID 1025 DEFAULT NULL, " +
        "DL CHAR(1) CCSID 1025 DEFAULT NULL, ACID CHAR(18) CCSID 1025 DEFAULT NULL, BOKC CHAR(2) CCSID 1025 DEFAULT NULL, " +
        "TRAT DECIMAL(5, 0) DEFAULT NULL, ACKEY VARCHAR(20) CCSID 1025 DEFAULT NULL) ");
      logger.info("������� ������� " + eodpopdTmp + ", �������� ��������� � �� ������ �� " + eodpopdSrc);
    } catch (SQLException ex) {
      logger.error("������ ��� ������� ������� ������� " + eodpopdTmp, ex);
      return false;
    }

    String sql1 = "insert into " + eodpopdTmp + " " +
        "(cnum, ccy, acod, acsq, pod, vald, pnar, psta, drcr, asoc, spos, brca, dpmt, voin, dlref, otrf, otst, ottp, pbrn, " +
        "batchpbr, dlno, dl, ACID, BOKC, TRAT, ACKEY) " +
        "select trim(cnum), ccy, right(digits(acod), 4), digits(acsq), date(pstd+719892), date(vald+719892), pnar, " +
        "(CASE WHEN DRCR=0 THEN -PSTA ELSE PSTA END) psta, drcr, case when asoc='' then '000000' else trim(asoc) end, " +
        "trim(spos), trim(brca), dpmt, voin, dlref, " +
        "CASE WHEN SPOS='  GE-FT' THEN LEFT(OTRF, 15) ELSE left(otrf, 6)||right(otrf, 9) END, " +
        "trim(otst), trim(E.ottp), " +
        "case when substr(trim(spos),1,3)<>'GE-' then 'BATCH' else trim(spos) end, " +
        "(COALESCE(MODULE, substr(trim(spos), 1, 3))) batchpbr, " +
        "left(otrf, 6) DLNO, " +
        "(COALESCE(" +
        "CASE WHEN SPOS='  GE-LE' AND (RIGHT(OTRF, 1)='F' OR RIGHT(OTRF, 1)='D') THEN 'C' ELSE " +
        "CASE WHEN SPOS='  GE-DL' AND MODULE='FX' THEN 'E' ELSE " +
        "CASE WHEN SPOS='  GE-DL' AND MODULE='FI' THEN 'I' ELSE " +
        "CASE WHEN SPOS='  GE-ST' THEN 'S' " +
        "END END END END, left(dlref, 1))) DL, " +
        "trim(cnum)||trim(ccy)||right(digits(acod), 4)||digits(acsq)||trim(brca) ACID, BOKC, " +
//        "where date(pstd+719892)='" + pod + "' "
        "";
    String sql2 = ", ACKEY from " + eodpopdSrc + " E LEFT JOIN " + schemaDWH + ".GCP_MDTYPE G ON E.OTTP=G.OTTP AND TRIM(E.SPOS)=G.PBR ";

    // ��������� ������ �� RUDWH/EODPOPD � RUBARS01/EODPOPD (� ���������� TRAT)
    String sql = sql1 + "TRAT" + sql2 + " WHERE HEX(TRAT)<>'4040404040'";
    int rq1 = st.executeUpdate(sql);
    logger.info(rq1 + " - " + sql);
    // ��������� ������ �� RUDWH/EODPOPD � RUBARS01/EODPOPD (� TRAT=4040404040)
    sql = sql1 + "0" + sql2 + " WHERE HEX(TRAT)='4040404040'";
    int rq2 = st.executeUpdate(sql);
    logger.info(rq2 + " - " + sql);
    logger.info("� ������� " + eodpopdTmp + " ���������� " + (rq1 + rq2) + " ������� �� " + eodpopdSrc + " ");
//    // ���������, �� �����-�� ������� �� ����� ������ � ACID ��������� �������������� DIGITS(ASOC) �� �� ���� ������, ���
//    // �������� � 27,08,2008, 20.10.2008 (������� ��� ������ �������������� ASOC), ������� ������ ���.
//    rq = st.executeUpdate("UPDATE " + eodpopdTmp + " SET ACID=CNUM||CCY||ACOD||ACSQ||BRCA");
//    logger.info("��������� " + rq + " ������� � " + eodpopdTmp + " ");

    /** ������� ������� �� ������� TMPEODPOPD */
    crtIndxsForEodpopd(st, eodpopdTmp);

    st.close();

    return true;
  }

  /**
   * ������� ������� ��� ������� <TMPEODPOPD>. <BR>
   * @param st java.sql.Statement ��� ���������� ��������; <BR>
   * @param tblName ������������ ��������� ������� TMPEODPOPD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void crtIndxsForEodpopd(java.sql.Statement st, String tblName) throws Exception {
    if (tblName.endsWith("TMPEODPOPD") || tblName.endsWith("TMPEODPOPDDL")) {
      st.execute("CREATE INDEX " + tblName + "_IDX1 ON " + tblName + " (DL, DLNO)");
//      logger.info("������� ������ " + tblName + "_IDX1");
    }
//    st.execute("CREATE INDEX " + tblName + "_IDX2 ON " + tblName + " (SPOS, OTRF)");
//    logger.info("������� ������ " + tblName + "_IDX2");
//    st.execute("CREATE INDEX " + tblName + "_IDX3 ON " + tblName + " (SPOS, OTST)");
//    logger.info("������� ������ " + tblName + "_IDX3");
//    st.execute("CREATE INDEX " + tblName + "_IDX4 ON " + tblName + " (SPOS, CCY)");
//    logger.info("������� ������ " + tblName + "_IDX4");
//    st.execute("CREATE INDEX " + tblName + "_IDX5 ON " + tblName + " (PBRN, CCY)");
//    logger.info("������� ������ " + tblName + "_IDX5");
//    st.execute("CREATE INDEX " + tblName + "_IDX6 ON " + tblName + " (PBRN, DLNO)");
//    logger.info("������� ������ " + tblName + "_IDX6");
//    st.execute("CREATE INDEX " + tblName + "_IDX7 ON " + tblName + " (PBRN, ASOC, CCY)");
//    logger.info("������� ������ " + tblName + "_IDX7");
//    st.execute("CREATE ENCODED VECTOR INDEX " + tblName + "_IDX8 ON " + tblName + " (ASOC, CCY)");
//    logger.info("������� ������ " + tblName + "_IDX8");
//    st.execute("CREATE INDEX " + tblName + "_IDX9 ON " + tblName + " (SPOS, PBRN)");
//    logger.info("������� ������ " + tblName + "_IDX9");
//    st.execute("CREATE INDEX " + tblName + "_IDX10 ON " + tblName + " (PBRN, BATCHPBR)");
//    logger.info("������� ������ " + tblName + "_IDX10");
    if (tblName.indexOf("TMPEODPOPDCE") != -1 || tblName.indexOf("TMPEODPOPDFT") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX11 ON " + tblName + " (OTRF)");
//      logger.info("������� ������ " + tblName + "_IDX11");
    }
    if (tblName.indexOf("TMPEODPOPDST") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX12 ON " + tblName + " (OTST)");
//      logger.info("������� ������ " + tblName + "_IDX12");
    }
    if (tblName.indexOf("TMPEODPOPDLE") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX13 ON " + tblName + " (SPOS, DL, DLNO)");
//      logger.info("������� ������ " + tblName + "_IDX13");
      st.execute("CREATE INDEX " + tblName + "_IDX14 ON " + tblName + " (SPOS, DL, OTST)");
//      logger.info("������� ������ " + tblName + "_IDX14");
      st.execute("CREATE INDEX " + tblName + "_IDX15 ON " + tblName + " (SPOS, DL, DLNO, OTST)");
//      logger.info("������� ������ " + tblName + "_IDX15");
      st.execute("CREATE INDEX " + tblName + "_IDX16 ON " + tblName + " (SPOS, DL, OTST, ASOC, DLNO)");
//      logger.info("������� ������ " + tblName + "_IDX16");
      st.execute("CREATE INDEX " + tblName + "_IDX17 ON " + tblName + " (DL, DLNO)");
//      logger.info("������� ������ " + tblName + "_IDX17");
    }
  }

  /**
   * ������� ������� ��� ������� <TMPEODPD>. <BR>
   * @param st java.sql.Statement ��� ���������� ��������; <BR>
   * @param tblName ������������ ��������� ������� TMPEODPD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void crtIndxsForEodpd(java.sql.Statement st, String tblName) throws Exception {
    if (tblName.indexOf("TMPEODPDLE") != -1 || tblName.indexOf("TMPEODPDDL") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX1 ON " + tblName + " (DL, DLNO)");
//      logger.info("������� ������ " + tblName + "_IDX1");
    }
//    st.execute("CREATE INDEX " + tblName + "_IDX2 ON " + tblName + " (DL, DLNO, FCNUM)");
//    logger.info("������� ������ " + tblName + "_IDX2");
//    st.execute("CREATE INDEX " + tblName + "_IDX3 ON " + tblName + " (OTRF)");
//    logger.info("������� ������ " + tblName + "_IDX3");
//    st.execute("CREATE INDEX " + tblName + "_IDX4 ON " + tblName + " (DLNO)");
//    logger.info("������� ������ " + tblName + "_IDX4");
//    st.execute("CREATE INDEX " + tblName + "_IDX5 ON " + tblName + " (OTST)");
//    logger.info("������� ������ " + tblName + "_IDX5");
    if (tblName.indexOf("TMPEODPDCE") != -1 || tblName.indexOf("TMPEODPDFT") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX6 ON " + tblName + " (OTRF)");
//      logger.info("������� ������ " + tblName + "_IDX6");
    }
    if (tblName.indexOf("TMPEODPDST") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX7 ON " + tblName + " (OTST)");
//      logger.info("������� ������ " + tblName + "_IDX7");
    }
    if (tblName.indexOf("TMPEODPDLE") != -1) {
      st.execute("CREATE INDEX " + tblName + "_IDX8 ON " + tblName + " (DL, FCNUM, DLNO)");
//      logger.info("������� ������ " + tblName + "_IDX8");
    }
  }

  /**
   * ���������� ������� ������� �� EODPOPD � PDLOAD (����� GTT). <BR>
   * @param pod ����, �� ������� ���������� ������; <BR>
   * @param tmpS ������������ ����� ��� ��������� ������� ��� ������; <BR>
   * @param tmpT ������������ ��������� ������� ��� ������; <BR>
   * @param podCurrates ����, �� ������� ����� ����� ��� �������� ������������; <BR>
   * @param where ������ ��� ������� ������ ������; <BR>
   * @return ���-�� ����������� ������� � PDLOAD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  public int doInsertPDLOAD(java.sql.Date pod, String tmpS, String tmpT, java.sql.Date podCurrates, String where) throws Exception {
    prepareData(tmpS, tmpT, pod, where == null || where.equals("") ? "ALL" : where, connection, podCurrates, "ALL", true);

    int rq = 0;
    connection.commit();
    return rq;
  }

  /**
   * ������� ��������� ������� � �������� � �� ������. <BR>
   * @param tmpS ������������ ����� ��� ��������� �������; <BR>
   * @param tmpT ������������ ��������� �������; <BR>
   * @param pod ����, �� ������� ��������� ������ �� ��������� �������; <BR>
   * @param pbr ������������ ������ ��� ALL - ��� ������; <BR>
   * @param conn Connection, ������� ������������ ��� ���������� ��������� ������� ������; <BR>
   * @param podCurrates ����, �� ������� ����� �����; <BR>
   * @param moduleName ������������ ������ ������������; <BR>
   * @param isDirect : true - ���� ��������� ���������� PDLOAD �������� �� EODPOPD, false - ����� EODPODEALS; <BR>
   * @return ���-�� ����������� �������; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private int prepareData(String tmpS, String tmpT, java.sql.Date pod, String pbr, java.sql.Connection conn, java.sql.Date podCurrates,
    String moduleName, boolean isDirect) throws Exception {

    java.sql.Statement st = conn.createStatement();
    /** �������� ��������� ������� ��� ������, ��� ������������ ����������� � PDLOAD */
    String cmd = "STRJRNPF FILE(" + tmpS + "/" + tmpT + ") JRN(" + schemaDWH.toUpperCase() + "/BARSJRN) IMAGES(*BOTH) OMTJRNE(*OPNCLO)";
    String cmd1 = "CLRPFM FILE(" + tmpS + "/" + tmpT + ")";
    lv.gcpartners.bank.util.CallCommandOnAS400 prgAS400 = new lv.gcpartners.bank.util.CallCommandOnAS400();
    String sqlGTT =
      (tmpS.substring(0, 3).equals("SES") ? "DECLARE GLOBAL TEMPORARY" : "CREATE") + " TABLE " + tmpS + "." + tmpT + " " +
      "(ID BIGINT, FXSC CHAR(3), FXPC CHAR(3), FXSA DECIMAL(19,0), FXPA DECIMAL(19,0), " +
      "FXBC CHAR(3), FTYP CHAR(3), FSEQ CHAR(2), DLVDAT DATE, DLMDAT DATE, PBR CHAR(7), BRCA CHAR(3), CNUM CHAR(6), CCY CHAR(3), " +
      "ACOD CHAR(4), ACSQ CHAR(2), PNAR VARCHAR(30), ASOC CHAR(6), AMNT DECIMAL(19,0), AMNTBC DECIMAL(19,0), POD DATE, VALD DATE, " +
      "DLTYPE CHAR(1), OTRF CHAR(15), OTTP CHAR(10), OTST CHAR(10), DPMT CHAR(3), DLID INTEGER, PDRF BIGINT, CPDRF BIGINT, POPIN CHAR(1), " +
      "OPCODE CHAR(3), ACID VARCHAR(18), BSAACID VARCHAR(20), BTYPE CHAR(1), RETAIL CHAR(10), DRCR CHAR(1), DLACSQ CHAR(2), " +
      "DLCNUM CHAR(6), FCNUM CHAR(6), ORIG_BORR CHAR(6), DLNO_ORIG CHAR(6), DLNO CHAR(6), DLID_ORIG INTEGER, BATCHID INTEGER, " +
      "DLORED DATE, DLVALUE DATE, DLVOIN CHAR(1), CNUMCE_DB CHAR(6), CNUMCE_CR CHAR(6), STATUS CHAR(1)) " +
      (tmpS.substring(0, 3).equals("SES") ? "NOT LOGGED ON COMMIT DELETE ROWS WITH REPLACE " : "");
//    try {
//      st.execute(sqlGTT);
      logger.info("������� ������� " + tmpS + "/" + tmpT + " - " + prgAS400.call(cmd1, null, null));
//    } catch (SQLException ex) {
//      if (ex.getErrorCode() != -601)
//        throw ex;
//      prgAS400.call(cmd, null, null);
//      logger.info("������� " + tmpS + "." + tmpT + " ����������, ������� = " + st.executeUpdate("DELETE FROM " + tmpS + "." + tmpT));
//    }
    st.close();
    conn.commit();
    prgAS400 = null;

    String sqlIns = null;
    int rq = 0;
    if (isDirect)
      sqlIns = getSQLOld(tmpS, tmpT, pod, pbr, podCurrates); // ����������� �� �������
    else
//      sqlIns = getSQL(pod, schTName, pbr, podCurrates, moduleName); // ����������� �� ������ (� �������������� EODPODEALS)
//    System.out.println(sqlIns);
//      System.out.println(pod);
//    PreparedStatement ps = conn.prepareStatement(sqlIns);
//    if (isDirect)
//      ps.setDate(1, pod);
//    int rq = ps.executeUpdate();
//    ps.close();
    logger.info("� " + tmpS + "." + tmpT + " ��������� " + rq + " �������");

    return rq;
  }

  /**
   * ����������, ������������ �� ��� �������� �������� � <PDLOAD> ������ ������ (����������� ���.������� <TMPEODPD>); <BR>
   * @param module ������������ ������; <BR>
   * @return ������� JOIN ���.������� ������ <TMPEODPD> � �������� ������������ <TMPEODPOPD> ��� �������� � <PDLOAD>. <BR>
   */
  private String isUsingDeals(String module) {
    String dealsJoin = null;
//    if (module.startsWith("IC") || module.equals("OTHER") || module.startsWith("BATCH") || module.startsWith("IER"))
//      isUsingDEALS = false;
    if (module.equals("FT"))
      dealsJoin = "P.otrf=deals.otrf";
    if (module.equals("CE"))
      dealsJoin = "P.otrf=deals.otrf";
    if (module.startsWith("LE"))
      dealsJoin = "P.dlno=deals.dlno and P.DL=deals.dl";
    if (module.startsWith("DL"))
      dealsJoin = "P.dlno=deals.dlno and P.DL=deals.dl";
    if (module.equals("ST"))
      dealsJoin = "P.otst=deals.otst";

    return dealsJoin;
  }

  /**
   * ���������� SQL select ��� ��������� ������ �� MIDAS, ��������������� �� ��� �������� � PDLOAD. <BR>
   * @param pod ����, �� ������� �������� ��� ������; <BR>
   * @param schTName �����+��� �������, ���� ����� ������; <BR>
   * @param eodpopdTmp �����+������������ ������� EODPOPD, ������ ��������� ������ � PDLOAD; <BR>
   * @param eodpodeals ��������� ������� ������ ������, ��� �������� � PDLOAD; <BR>
   * @param accntabTmp ��������� ������� ������ ACCNTAB; <BR>
   * @param whereSQL ������� ������� ������������ �� �������� ������; <BR>
   * @param module ������������ ������ ��� ������� ������; <BR>
   * @param minID ����������� �� ������������, ���������� � PDLOAD � ������� ������; <BR>
   * @param npp ����� ����������� SQL �� �������; <BR>
   * @return String �������������� insert � select�� �� ������. <BR>
   */
  private synchronized String getSQL(java.sql.Date pod, String schTName, String eodpopdTmp, String eodpodeals, String accntabTmp,
               String whereSQL, String module, long minID, int npp) {

    // ��� ��������� ������� ������� ������ � DEALS (EODPODEALS): FT, CE, DL, LE, ST
    // ��� ��������� ������� �� ������� ������ � DEALS (EODPODEALS): IC, OTHER, BATCH*, IER*
    // ��� ��������� � ���������� ������ ������������ ������, ���������� �������� ��� ������������ ����� (!!!)
    boolean isUsingDEALS = isUsingDeals(module) != null;

    /** FXSC */
//    String fxsc = "value(";
//    fxsc = fxsc + (moduleName.equals("FT") || moduleName.equals("ALL") ? "i.smcy,o.smcy,n.ccy," : "");
//    fxsc = fxsc + (moduleName.equals("CE") || moduleName.equals("ALL") ? "cu.drcy," : "");
//    fxsc = fxsc + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.FXSC," : "");
//    fxsc = fxsc + "cast(null as char(3))) as fxsc";
    String fxsc = "DEALS.FXSC";
    if (!isUsingDEALS)
      fxsc = "CAST(NULL AS CHAR(3)) AS FXSC";
    /** FXPC */
//    String fxpc = "value(";
//    fxpc = fxpc + (moduleName.equals("FT") || moduleName.equals("ALL") ? "i.pccy,o.pccy,n.ccy," : "");
//    fxpc = fxpc + (moduleName.equals("CE") || moduleName.equals("ALL") ? "cu.crcy," : "");
//    fxpc = fxpc + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.FXPC," : "");
//    fxpc = fxpc + "cast(null as char(3)),cast(null as char(3))) as fxpc";
    String fxpc = "DEALS.FXPC";
    if (!isUsingDEALS)
      fxpc = "CAST(NULL AS CHAR(3)) AS FXPC";
    /** FXSA */
//    String fxsa = "value(";
//    fxsa = fxsa + (moduleName.equals("FT") || moduleName.equals("ALL") ? "i.smam,o.smam,n.amnt," : "");
//    fxsa = fxsa + (moduleName.equals("CE") || moduleName.equals("ALL") ? "cu.dram," : "");
//    fxsa = fxsa + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.FXSA," : "");
//    fxsa = fxsa + "cast(null as decimal(19,0)),cast(null as decimal(19,0))) as fxsa";
    String fxsa = "DEALS.FXSA";
    if (!isUsingDEALS)
      fxsa = "CAST(NULL AS DECIMAL(19, 0)) AS FXSA";
    /** FXPA */
//    String fxpa = "value(";
//    fxpa = fxpa + (moduleName.equals("FT") || moduleName.equals("ALL") ? "i.pyam,o.pyam,n.amnt," : "");
//    fxpa = fxpa + (moduleName.equals("CE") || moduleName.equals("ALL") ? "cu.cram," : "");
//    fxpa = fxpa + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.FXPA," : "");
//    fxpa = fxpa + "cast(null as decimal(19,0)),cast(null as decimal(19,0))) as fxpa";
    String fxpa = "DEALS.FXPA";
    if (!isUsingDEALS)
      fxpa = "CAST(NULL AS DECIMAL(19, 0)) AS FXPA";
    /** FXBC */
//    String fxbc = "value(";
//    fxbc = fxbc + (moduleName.equals("FT") || moduleName.equals("ALL") ? "i.brca,o.brca,n.brca," : "");
//    fxbc = fxbc + (moduleName.equals("CE") || moduleName.equals("ALL") ? "cu.brca," : "");
//    fxbc = fxbc + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.fxbc," : "");
//    fxbc = fxbc + "cast(null as char(3)),cast(null as char(3))) as fxbc";
    String fxbc = "DEALS.FXBC";
    if (!isUsingDEALS)
      fxbc = "CAST(NULL AS CHAR(3)) AS FXBC";
    /** FTYP */
//    String ftyp = "value(";
//    ftyp = ftyp + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.ftyp," : "");
//    ftyp = ftyp + "cast(null as char(3)),cast(null as char(3))) as ftyp";
    String ftyp = "DEALS.FTYP";
    if (!isUsingDEALS)
      ftyp = "CAST(NULL AS CHAR(3)) AS FTYP";
    /** FSEQ */
//    String fseq = "value(";
//    fseq = fseq + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.fseq," : "");
//    fseq = fseq + "cast(null as char(2)),cast(null as char(2))) as fseq";
    String fseq = "DEALS.FSEQ";
    if (!isUsingDEALS)
      fseq = "CAST(NULL AS CHAR(2)) AS FSEQ";
    /** DLVDAT */
//    String dlvdat = "value(";
//    dlvdat = dlvdat + (moduleName.equals("ST") || moduleName.equals("ALL") ? "date(sectyd.itld+719892)," : "");
//    dlvdat = dlvdat + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlvdat," : "");
//    dlvdat = dlvdat + "cast(null as date),cast(null as date)) as dlvdat";
    String dlvdat = "DEALS.DLVDAT";
    if (!isUsingDEALS)
      dlvdat = "CAST(NULL AS DATE) AS DLVDAT";
    /** DLMDAT */
//    String dlmdat = "value(";
//    dlmdat = dlmdat + (moduleName.equals("ST") || moduleName.equals("ALL") ? "date(sectyd.maty+719892)," : "");
//    dlmdat = dlmdat + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlmdat," : "");
//    dlmdat = dlmdat + "cast(null as date),cast(null as date)) as dlmdat";
    String dlmdat = "DEALS.DLMDAT";
    if (!isUsingDEALS)
      dlmdat = "CAST(NULL AS DATE) AS DLMDAT";
    /** DLID */
//    String dlid = "value(";
//    dlid = dlid + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlid," : "");
//    dlid = dlid + "cast(null as integer),cast(null as integer)) as dlid";
    String dlid = "DEALS.DLID";
    if (!isUsingDEALS)
      dlid = "CAST(NULL AS INTEGER) AS DLID";
    /** DLACSQ */
//    String dlacsq = "value(";
//    dlacsq = dlacsq + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlacsq," : "");
//    dlacsq = dlacsq + "cast(null as char(2)),cast(null as char(2))) as dlacsq";
    String dlacsq = "DEALS.DLACSQ";
    if (!isUsingDEALS)
      dlacsq = "CAST(NULL AS CHAR(2)) AS DLACSQ";
    /** DLCNUM */
//    String dlcnum = "value(";
//    dlcnum = dlcnum + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlcnum," : "");
//    dlcnum = dlcnum + "cast(null as char(6)),cast(null as char(6))) as dlcnum";
    String dlcnum = "DEALS.DLCNUM";
    if (!isUsingDEALS)
      dlcnum = "CAST(NULL AS CHAR(6)) AS DLCNUM";
    /** FCNUM */
//    String fcnum = "value(";
//    fcnum = fcnum + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.fcnum," : "");
//    fcnum = fcnum + "cast(null as char(6)),cast(null as char(6))) as fcnum";
    String fcnum = "DEALS.FCNUM";
    if (!isUsingDEALS)
      fcnum = "CAST(NULL AS CHAR(6)) AS FCNUM";
    /** ORIG_BORR */
//    String orig_borr = "value(";
//    orig_borr = orig_borr + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.orig_borr," : "");
//    orig_borr = orig_borr + "cast(null as char(6)),cast(null as char(6))) as orig_borr";
    String orig_borr = "DEALS.ORIG_BORR";
    if (!isUsingDEALS)
      orig_borr = "CAST(NULL AS CHAR(6)) AS ORIG_BORR";
    /** DLNO_ORIG */
//    String dlno_orig = "value(";
//    dlno_orig = dlno_orig + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlno_orig," : "");
//    dlno_orig = dlno_orig + "cast(null as char(6)),cast(null as char(6))) as dlno_orig";
    String dlno_orig = "DEALS.DLNO_ORIG";
    if (!isUsingDEALS)
      dlno_orig = "CAST(NULL AS CHAR(6)) AS DLNO_ORIG";
    /** DLNO */
//    String dlno = "value(";
//    dlno = dlno + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlno," : "");
//    dlno = dlno + "cast(null as char(6)),cast(null as char(6))) as dlno";
    // �� �� DLTYP=C(��� ������ EODPOPD � EODPOPD �� DLNO), ������ ���
    String dlno = "(SELECT DLNO FROM " + schemaDWH + ".DELOTAB WHERE ID=DEALS.DLID)";
//    String dlno = "DEALS.DLNO";
    if (!isUsingDEALS)
      dlno = "CAST(NULL AS CHAR(6)) AS DLNO";
    /** DLID_ORIG */
//    String dlid_orig = "value(";
//    dlid_orig = dlid_orig + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlid_orig," : "");
//    dlid_orig = dlid_orig + "cast(null as integer),cast(null as integer)) as dlid_orig";
    String dlid_orig = "DEALS.DLID_ORIG";
    if (!isUsingDEALS)
      dlid_orig = "CAST(NULL AS INTEGER) AS DLID_ORIG";
    /** DLORED */
//    String dlored = "value(";
//    dlored = dlored + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlored," : "");
//    dlored = dlored + "cast(null as date),cast(null as date)) as dlored";
    String dlored = "DEALS.DLORED";
    if (!isUsingDEALS)
      dlored = "CAST(NULL AS DATE) AS DLORED";
    /** DLVALUE */
//    String dlvalue = "value(";
//    dlvalue = dlvalue + (moduleName.equals("ST") || moduleName.equals("ALL") ? "date(sectyd.itld+719892)," : "");
//    dlvalue = dlvalue + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlvalue," : "");
//    dlvalue = dlvalue + "cast(null as date),cast(null as date)) as dlvalue";
    String dlvalue = "DEALS.DLVALUE";
    if (!isUsingDEALS)
      dlvalue = "CAST(NULL AS DATE) AS DLVALUE";
    /** DLMATUR */
//    String dlmatur = "value(";
//    dlvalue = dlmatur + (moduleName.equals("ST") || moduleName.equals("ALL") ? "date(sectyd.maty+719892)," : "");
//    dlvalue = dlmatur + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "DEALS.dlmatur," : "");
//    dlvalue = dlmatur + "cast(null as date),cast(null as date)) as dlmatur";
    String dlmatur = "DEALS.DLMATUR";
    if (!isUsingDEALS)
      dlmatur = "CAST(NULL AS DATE) AS DLMATUR";
    /** DLVOIN */
//    String dlvoin = "";
//    dlvoin = dlvoin + (moduleName.equals("LE") || moduleName.equals("DL") || moduleName.equals("ALL") ? "case when deals.reci='R' and e.voin=1 then 'E' else '' end" : "");
//    dlvoin = dlvoin + (!moduleName.equals("LE") && !moduleName.equals("DL") && !moduleName.equals("ALL") ? "''" : "");
//    dlvoin = dlvoin + " as dlvoin";
    String dlvoin = "(CASE WHEN DEALS.RECI IS NOT NULL AND DEALS.RECI='R' AND P.VOIN=1 THEN 'E' ELSE CAST(NULL AS CHAR(1)) END) AS DLVOIN";
    if (!isUsingDEALS)
      dlvoin = "CAST(NULL AS CHAR(1)) AS DLVOIN";
    /** CNUMCE_DB */
//    String cnumce_db = "value(";
//    cnumce_db = cnumce_db + (moduleName.equals("CE") || moduleName.equals("ALL") ? "SUBSTR(CHAR(cu.dcus+1000000),2,6)," : "");
//    cnumce_db = cnumce_db + "cast(null as char(6)),cast(null as char(6))) as cnumce_db";
    String cnumce_db = "DEALS.CNUMCE_DB";
    if (!isUsingDEALS)
      cnumce_db = "CAST(NULL AS CHAR(6)) AS CNUMCE_DB";
    /** CNUMCE_CR */
//    String cnumce_cr = "value(";
//    cnumce_cr = cnumce_cr + (moduleName.equals("CE") || moduleName.equals("ALL") ? "SUBSTR(CHAR(cu.ccus+1000000),2,6)," : "");
//    cnumce_cr = cnumce_cr + "cast(null as char(6)),cast(null as char(6))) as cnumce_cr";
    String cnumce_cr = "DEALS.CNUMCE_CR";
    if (!isUsingDEALS)
      cnumce_cr = "CAST(NULL AS CHAR(6)) AS CNUMCE_CR";

    // INSERT� � SELECT��� ��� ����������� �������� <TMPEODPOPD> ---> <PDLOAD>
    String sqlIns = "INSERT INTO " + schTName + " " +
        "(id, fxsc, fxpc, fxsa, fxpa, fxbc, ftyp, fseq, dlvdat, dlmdat, pbr, brca, cnum, ccy, acod, acsq, " +
        "pnar, asoc, amnt, amntbc, pod, vald, dltype, otrf, ottp, otst, DPMT, dlid, pdrf, cpdrf, popin, opcode, " +
        "acid, bsaacid, btype, retail, drcr, DLACSQ, dlcnum, fcnum, orig_borr, dlno_orig, dlno, dlid_orig, batchid, " +
        /******************************** ��������� 26.12.2005 ��������� �.�. ***************************/
        "dlored, dlvalue, dlvoin, cnumce_db, cnumce_cr, status, BOKC, TRAT, ACKEY " +
        (module.startsWith("FT") || module.startsWith("CE") || module.startsWith("BATCH") || module.startsWith("DL") ? ", PNARR, ID_DOC" : "") +
        (curVer >= 153 ? ", DLMATUR" : "") +
        ") " +
        /************************************************************************************************/
//    String sqlIns =
//        "select count(*) from ( " +
        "select " +
        /***** ��������� 27.02.2007 *****/
//        "NEXT VALUE FOR " + schemaDWH + ".PDLOAD_SEQ, " +
//        "-1, " +
        "(ROW_NUMBER() OVER ()) - 1 + " + minID + " ID, " +
        fxsc + "," + fxpc + "," + fxsa + "," + fxpa + "," + fxbc + "," + ftyp + "," + fseq + "," + dlvdat + "," + dlmdat + "," +
        "P.SPOS as pbr, P.BRCA as brca, P.cnum as cnum, P.CCY as ccy, P.acod as acod, P.acsq as acsq, " +
        "P.PNAR as pnar, P.asoc as asoc, P.PSTA as AMNT, " +

        "(SELECT ROUND(P.PSTA*cr.rate*cr.amnt, 0) FROM " + schemaDWH + ".CURRATES CR WHERE CR.CCY=P.CCY AND CR.DAT='" + pod.toString() + "') as AMNTBC, " +
//        "P.AMNTBC, " +

        "P.POD as POD, P.VALD as VALD, " +

//        "case when length(P.ottp)=3 and length(P.otst)=0 then 'C' else P.DL end as DTYPE, " + // 10.04.2010 - ��� � EODPOPD ������������� DL=C
        "(CASE WHEN P.DL='E' THEN 'D' ELSE P.DL END) as DTYPE, " + // ������������ FX --> DL(MM,FX)

        "P.OTRF, P.OTTP, P.OTST, P.DPMT, " +
        dlid + ", " +
        "0 as pdrf, 0 as cpdrf, '' as popin, '' as opcode, P.ACID as acid, '' as bsaacid, " +

	//����� ��������
		"(SELECT MAX(BS.TYPE) BTYPE FROM " + schemaDWH + ".BSS BS, " + schemaDWH + ".SDACODPD SD2 " +
		"	WHERE " +
		"		P.ACOD=SD2.A5ACCD AND " +
		"		P.POD BETWEEN BS.DAT AND BS.DATTO " +
		"		AND " +
		"		TRIM(COALESCE(CASE WHEN P.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END,'')) = " +
		"			CASE " +
		"				WHEN LENGTH(TRIM(COALESCE(CASE WHEN P.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END,'')))=0 THEN 'X' " +
		"				ELSE SUBSTR(BS.ACC2, 1, LENGTH(TRIM(COALESCE(CASE WHEN P.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END,'')))) " +
		"			END " +
		"	HAVING MAX(COALESCE(BS.TYPE,''))=MIN(COALESCE(BS.TYPE,'')) " +
		") as btype, " +		

        // "(select MAX(type) from " + schemaDWH + ".bss bs, " + schemaDWH + ".sdacodpd sd2 " +
        // "where sd2.a5accd=P.acod and '" + pod + "' between bs.dat and bs.datto and " +
        // "left(bs.acc1, 3)=" +
        // "(case when left(bs.acc1,1)='9' then " +
        // "SUBSTR((CASE WHEN P.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END), 1, 3) " +
        // "else " +
        // "SUBSTR((CASE WHEN P.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END), 1, 1) || substr(bs.acc1, 2, 2) " +
        // "end) " +
        // ") as btype, " +

//        "P.BTYPE, " +

        "CASE WHEN acc.acno=0 THEN '' ELSE DIGITS(acc.acno) END as retail, " +
        "CASE WHEN P.DRCR='0' THEN 'D' ELSE 'C' END as drcr, " +
        dlacsq + ", " + dlcnum + ", " + fcnum + ", " + orig_borr + ", " + dlno_orig + ", " + dlno + ", " + dlid_orig + ", " +
        "case when P.pbrn='BATCH' then 1 else 0 end as batchid, " +
        dlored + ", " + dlvalue + ", " + dlvoin + ", " + cnumce_db + ", " + cnumce_cr + ", " + "'I' as status, P.BOKC, P.TRAT, P.ACKEY " +

        (module.startsWith("BATCH") ?
//        ",value(P2B.DETAILS, EXTP.EXTDATA, TRIM(TRTL.TRATNM) || ' ' || TRIM(P.PNAR), TRIM(SDRR.A1RTNM) || ' ' || TRIM(P.PNAR), TRIM(P.PNAR)) AS PNARR " :
        ",value(P2B.DETAILS, EXTP.EXTDATA, " +
        "CASE WHEN TRIM(TRTL.TRATNM)=TRIM(P.PNAR) THEN TRIM(P.PNAR) ELSE TRIM(TRTL.TRATNM) || ' ' || TRIM(P.PNAR) END, " +
        "CASE WHEN TRIM(SDRR.A1RTNM)=TRIM(P.PNAR) THEN TRIM(P.PNAR) ELSE TRIM(SDRR.A1RTNM) || ' ' || TRIM(P.PNAR) END, TRIM(P.PNAR)) AS PNARR " +
        ", P2B.ID AS ID_DOC " +
        ""
        :
//        (module.startsWith("FT") || module.startsWith("CE") ? ", '' AS PNARR "
        (module.startsWith("FT") || module.startsWith("CE") || module.startsWith("DL") ? ", P2B.DETAILS AS PNARR, P2B.ID AS ID_DOC "
        :
        "")) +

        (curVer >= 153 ? ", " + dlmatur : "") + " " +

        "from " + eodpopdTmp + " P ";

        /** ACCNTAB */
        sqlIns = sqlIns + "left join " + accntabTmp + " acc on P.acid=acc.acid ";

        // ��� ����� ������������ ������ � ����������� ������, ������� ����� (!!!)
        sqlIns = sqlIns +
        (module.equals("FT") ?
        "left join " + eodpodeals + " deals on P.otrf=deals.otrf and P.spos='GE-FT' " +
        "left join " + schemaDWH + ".TMPP2BLST P2B on p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.spos and " +
          "p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and p2b.acsq=p.acsq and p2b.BRCA=p.brca and " +
          "p2b.PSTA=p.psta and p2b.DRCR=p.drcr and p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and " +
          "p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.DLREF=p.dlref and p2b.OTTP=p.ottp and p2b.OTST=p.otst and " +
          "p2b.BOKC=p.bokc " +
        ""
        :
        "");

        // ��� ����� ������������ ������ � ����������� ������, ������� ����� (!!!)
        sqlIns = sqlIns +
        (module.equals("CE") ?
        "left join " + eodpodeals + " deals on P.otrf=deals.otrf and P.spos='GE-CE' " +
        "left join " + schemaDWH + ".TMPP2BLST P2B on p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.spos and " +
          "p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and p2b.acsq=p.acsq and p2b.BRCA=p.brca and " +
          "p2b.PSTA=p.psta and p2b.DRCR=p.drcr and p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and " +
          "p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.DLREF=p.dlref and p2b.OTTP=p.ottp and p2b.OTST=p.otst and " +
          "p2b.BOKC=p.bokc " +
        ""
        :
        "");

        // ��� ����� ������������ ������ � ����������� ������, ������� ����� (!!!)
        sqlIns = sqlIns +
        (module.startsWith("DL") ?
        "left join " + eodpodeals + " deals on P.dlno=deals.dlno and P.DL=deals.dl AND P.SPOS='GE-DL' " +
        "left join " + schemaDWH + ".TMPP2BLST P2B on p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.spos and " +
          "p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and p2b.acsq=p.acsq and p2b.BRCA=p.brca and " +
          "p2b.PSTA=p.psta and p2b.DRCR=p.drcr and p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and " +
          "p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.DLREF=p.dlref and p2b.OTTP=p.ottp and p2b.OTST=p.otst and " +
          "p2b.BOKC=p.bokc "
        :
        "");

        // ��� ����� ������������ ������ � ����������� ������, ������� ����� (!!!)
        sqlIns = sqlIns +
        (module.equals("ST") ?
        "left join " + eodpodeals + " deals on P.otst=deals.otst and P.spos='GE-ST' " :
        "");

        // ��� ����� ������������ ������ � ����������� ������, ������� ����� (!!!)
        sqlIns = sqlIns +
        (module.startsWith("BATCH") ?
        "left join " + schemaDWH + ".TMPP2BLST P2B on p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.spos and " +
          "p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and p2b.acsq=p.acsq and p2b.BRCA=p.brca and " +
          "p2b.PSTA=p.psta and p2b.DRCR=p.drcr and p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and " +
          "p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.DLREF=p.dlref and p2b.OTTP=p.ottp and p2b.OTST=p.otst and " +
          "p2b.BOKC=p.bokc " +
        "left join " + schemaDWH + ".extpostdtl extp on extp.tdat=p.pod and extp.pbr=p.spos " +
        "left join " + schemaDWH + ".trtlstpf trtl on trtl.trat=p.trat " +
        "left join " + schemaDWHIN + ".sdretrpd sdrr on sdrr.A1RTTY=p.trat " +
        ""
        :
        "");

        // ��� ����� ������������ ������ � ����������� ������, ������� ����� (!!!)
        if (module.startsWith("LE")) {
          // ��� �� Past Due �� ������� ������ ��� ��� LENGTH(OTTP)=3 � LENGTH(OTST)=0
          if (this.curVer >= 155) {
            // ������� �� ��������, �� ������� ��� ��������� ����������
            if (npp == 0) {
              sqlIns = sqlIns + // ��� ������ ������ �� LE
                "LEFT JOIN " + eodpodeals + " DEALS ON P.dlno=deals.dlno and P.DL=deals.dl " +
                "WHERE NOT EXISTS (SELECT * FROM " + eodpodeals + " DEALS WHERE P.DLNO=DEALS.DLNO AND P.DL=DEALS.dl) AND P.SPOS='GE-LE' " +
                (whereSQL == null || whereSQL.equals("") ? "" : " AND " + whereSQL);
            }
            if (npp == 1) {
              sqlIns = sqlIns + // ��� ��������
                "join " + eodpodeals + " deals on P.dlno=deals.dlno and P.DL=deals.dl AND P.ASOC=DEALS.FCNUM AND " +
                "RIGHT(P.OTRF, 3)=TRIM(DEALS.OTRF)||RIGHT(P.OTRF, 1) AND DEALS.CCY=P.CCY " +
                "WHERE DEALS.DLID IS NOT NULL AND P.SPOS='GE-LE' AND P.DL='C' AND P.OTST='' " +
                (whereSQL == null || whereSQL.equals("") ? "" : " AND " + whereSQL);
            }
            if (npp == 2) {
              sqlIns = sqlIns + // ��� ������
                "join " + eodpodeals + " deals on P.dlno=deals.dlno and P.DL=deals.dl AND " +
                "RIGHT(P.OTRF, 3)=TRIM(DEALS.OTRF)||RIGHT(P.OTRF, 1) AND DEALS.CCY=P.CCY " +
                "WHERE DEALS.DLID IS NOT NULL AND P.SPOS='GE-LE' AND P.DL='C' AND P.OTST<>'' " +
                (whereSQL == null || whereSQL.equals("") ? "" : " AND " + whereSQL);
            }
            if (npp == 3) {
              sqlIns = sqlIns + // ��������� LE
                "join " + eodpodeals + " deals on P.dlno=deals.dlno and P.DL=deals.dl " +
                "WHERE DEALS.DLID IS NOT NULL AND P.SPOS='GE-LE' AND P.DL<>'C' " +
                (whereSQL == null || whereSQL.equals("") ? "" : " AND " + whereSQL);
            }
          } else {
            sqlIns = sqlIns +
              "left join " + eodpodeals + " deals on P.dlno=deals.dlno and P.DL=deals.dl AND P.SPOS='GE-LE' " +
                "and not (length(P.ottp)=3 and length(P.otst)=0) ";
            sqlIns = sqlIns + (whereSQL == null || whereSQL.equals("") ? "" : "WHERE " + whereSQL);
          }
        } else {
          sqlIns = sqlIns + (whereSQL == null || whereSQL.equals("") ? "" : "WHERE " + whereSQL);
        }

    return sqlIns;
  }

  /**
   * ���������� SQL select ��� ��������� ������ �� MIDAS, ��������������� �� ��� �������� � PDLOAD. <BR>
   * @param tmpS ����� ��� �������, ���� ����� ������; <BR>
   * @param tmpT �������, ���� ����� ������; <BR>
   * @param pod ����, �� ������� �������� ��� ������; <BR>
   * @param podCurrates ����, �� ������� ����� �����; <BR>
   * @param pbr ������ �� �������� �������� ��� ������ ��� ALL - ��� ������; <BR>
   * @return �������������� select. <BR>
   */
  private String getSQLOld(String tmpS, String tmpT, java.sql.Date pod, String pbr, java.sql.Date podCurrates) {
    //zagruzka novih dannih EODPOPD->PDLOAD
    String sqlIns = "INSERT INTO " + tmpS + "." + tmpT + " " +
        "(id, fxsc, fxpc, fxsa, fxpa, fxbc, ftyp, fseq, dlvdat, dlmdat, pbr, brca, cnum, ccy, acod, acsq, " +
        "pnar, asoc, amnt, amntbc, pod, vald, dltype, otrf, ottp, otst, DPMT, dlid, pdrf, cpdrf, popin, opcode," +
        "acid, bsaacid, btype, retail, drcr, DLACSQ, dlcnum, fcnum, orig_borr, dlno_orig, dlno, dlid_orig, batchid," +
        /******************************** ��������� 26.12.2005 ��������� �.�. ***************************/
        "dlored, dlvalue, dlvoin, cnumce_db, cnumce_cr, status) " +
        /************************************************************************************************/
//    String sqlIns =
//        "select count(*) from ( " +
        "select " +
        /***** ��������� 27.02.2007 *****/
        "NEXT VALUE FOR " + schemaDWH + ".PDLOAD_SEQ, " +
        "value(i.smcy,o.smcy,n.ccy,dextab.pucy,cu.drcy,DELOTAB.CCY) as ftsc," +
        "value(i.pccy,o.pccy,n.ccy,dextab.slcy,cu.crcy,DELOTAB.CCY) as ftpc," +
        "value(i.smam,o.smam,n.amnt,dextab.puam,cu.dram,DEALTAB.RODA) as ftsa," +
        "value(i.pyam,o.pyam,n.amnt,dextab.slam,cu.cram,DEALTAB.RBDA) as ftpa," +
        "value(LOANTAB.BRCA,DEALTAB.BRCA,i.brca,o.brca,n.brca,dextab.brca,cu.brca) as ftbc," +
        "loantab.ftyp," +
        "loantab.fseq," +
        "value(loantab.vdat,dealtab.vdat,dextab.ddat,date(sectyd.itld+719892)) as dlvdat," +
        "value(loantab.mdat,dealtab.mdat,dextab.vdat,date(sectyd.maty+719892)) as dlmdat," +
        "e.SPOS as pbr, " +
        "e.BRCA, " +
        "e.cnum as cnum, " +
        "e.CCY as ccy, " +
        "e.acod as acod, " +
        "e.acsq as acsq, " +
        "CAST(e.PNAR as char(30) for bit data) as pnar, " +
        "e.asoc as asoc, " +
        "e.PSTA as AMNT, " +
        "ROUND(e.PSTA*cr.rate*cr.amnt,0) as AMNTBC, " +
        "e.POD as POD, " +
        "e.VALD as VALD, " +
        "case when length(e.ottp)=3 and length(e.otst)=0 then 'C' else SUBSTR(e.DLREF,1,1) end as DTYPE, " +
        "e.OTRF, " +
        "e.OTTP, " +
        "e.OTST, " +
        "e.DPMT, " +
        "delotab.id as dlid, " +
        "0 as pdrf," +
        "0 as cpdrf," +
        "'' as popin," +
        "'' as opcode," +
        "e.cnum||e.ccy||e.acod||e.acsq||e.brca as acid," +
        "'' as bsaacid," +
		
	//����� ��������
		"(SELECT MAX(BS.TYPE) BTYPE FROM " + schemaDWH + ".BSS BS, " + schemaDWH + ".SDACODPD SD2 " +
		"	WHERE " +
		"		E.ACOD=SD2.A5ACCD AND " +
		"		E.POD BETWEEN BS.DAT AND BS.DATTO " +
		"		AND " +
		"		TRIM(COALESCE(CASE WHEN E.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END,'')) = " +
		"			CASE " +
		"				WHEN LENGTH(TRIM(COALESCE(CASE WHEN E.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END,'')))=0 THEN 'X' " +
		"				ELSE SUBSTR(BS.ACC2, 1, LENGTH(TRIM(COALESCE(CASE WHEN E.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END,'')))) " +
		"			END " +
		"	HAVING MAX(COALESCE(BS.TYPE,''))=MIN(COALESCE(BS.TYPE,'')) " +
		") as btype, " +		
		
		
        // "(" +

        // "select distinct bs.type from " + schemaDWH + ".bss bs, " + schemaDWH + ".sdacodpd sd2 " +
        // "where sd2.a5accd=e.acod and '" + pod.toString() + "' between bs.dat and bs.datto and " +
        // "left(bs.acc1, 3)=" +
        // "(case when left(bs.acc1,1)='9' then " +
        // "SUBSTR((CASE WHEN e.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END), 1, 3) " +
        // "else " +
        // "SUBSTR((CASE WHEN e.CCY='RUR' THEN SD2.A5SRLC ELSE SD2.A5SRFC END), 1, 1) || substr(bs.acc1, 2, 2) " +
        // "end) " +
        // ") as btype, " +
        "CASE WHEN acc.acno=0 THEN '' ELSE SUBSTR(CHAR(acc.acno+10000000000),2,10)  END as retail, " +
        "CASE WHEN e.DRCR='0' THEN 'D' ELSE 'C' END as drcr, " +
        "dealtab.cdas as DLACSQ, " +
        "value(loantab.cnum, dealtab.cnum, dextab.cnum) as dlcnum, " +
        "loantab.fcus as fcnum, " +
        "loantab.olno as orig_borr, " +
        "(select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=delotab.origid) as dlno_orig, " +
        "delotab.dlno as dlno, " +
        "delotab.origid as dlid_orig, " +
        "case when e.pbrn='BATCH' then 1 else 0 end as batchid, " +
        /******************** ��������� 26.12.2005 ��������� �.�. ***************************/
        "value(loantab.ored, dealtab.ored), " +  //value(loantab.vdat, dealtab.vdat)
        "value(loantab.vdat, (select dealtab2.vdat from " + schemaDWH + ".delotab delotab2, "+ schemaDWH + ".dealtab dealtab2 " +
        "where delotab2.id=delotab.origid and dealtab2.dealid=delotab.origid and delotab2.dat=dealtab2.dat), dextab.ddat, date(sectyd.itld+719892)) as dlvalue, " +
        "case when delotab.reci='R' and e.voin=1 then 'E' else '' end as dlvoin, " +
        "SUBSTR(CHAR(cu.dcus+1000000),2,6) as cnumce_db, SUBSTR(CHAR( cu.ccus+1000000),2,6) as cnumce_cr, " +
        "'I' " +
        /************************************************************************************/
        "from " + schemaDWH + ".eodpopd e " +
        "inner join " + schemaDWH +  ".currates cr on cr.ccy=e.ccy and cr.dat='" + podCurrates.toString() + "' " +
        "left join " + schemaDWHIN + ".inpaydd i on i.pref=e.otrf and e.spos='GE-FT' " +
        "left join " + schemaDWHIN + ".otpaydd o on o.pref=e.otrf and e.spos='GE-FT' " +
        "left join " + schemaDWHIN + ".ntrandd n on n.tfrf=e.otrf and e.spos='GE-FT' " +
        "left join " + schemaDWHIN + ".CUSEXCE cu on cu.ipdn=e.otrf and e.spos='GE-CE' " +
        "left join " + schemaDWH + ".delotab delotab on substr(e.otrf,1,6)=delotab.dlno and ? between delotab.dat and delotab.datto " +
        "and SUBSTR(e.DLREF,1,1)=case when delotab.dl='E' then 'D' else delotab.dl end " +
        "and not (length(e.ottp)=3 and length(e.otst)=0) " + // �� ������� ������ ��� ��� LENGTH(OTTP)=3 � LENGTH(OTST)=0
        "left join " + schemaDWH + ".loantab loantab on delotab.id=loantab.loanid and loantab.datto=delotab.datto " +
        "left join " + schemaDWH + ".dealtab dealtab on delotab.id=dealtab.dealid and dealtab.datto=delotab.datto " +
        "left join " + schemaDWH + ".dextab dextab on delotab.id=dextab.dealid and delotab.datto=dextab.datto " +
        "left join " + schemaDWHIN + ".sectyd sectyd on e.otst=sectyd.sesn and e.spos='GE-ST' " +
        "left join " + schemaDWH + ".accntab acc on e.cnum=digits(acc.cnum) and e.ccy=acc.ccy and e.acod=digits(acc.acod) and e.acsq=digits(acc.acsq) and e.brca=acc.brca " +
        "WHERE " + (pbr.equals("ALL") ? "" : " " + pbr.trim());

      System.out.println(sqlIns);

    return sqlIns;
  }

  class SavePostings extends Thread {
    java.sql.Connection conn;
    java.sql.Date pod; // ����, �� ������� ����� �����
    boolean isConnInner = true, finished = false;
    Object[] tmpT = null;
    String schTName, whereSQL, module, eodpopdTmp, eodpodeals, accntabTmp, error;
    ProcessRB cPRB;
    int rqR = 0, rqDR = 0;
    long minID = 0;
    String dealsJoin = null;

    public SavePostings(java.sql.Connection conn, String schTName, String eodpopdTmp, java.sql.Date pod, String whereSQL, String module,
               String eodpodeals, String accntabTmp, ProcessRB cPRB) throws Exception {
      logger.info("Starting " + module + " --- ");
      if (conn == null) {
        this.conn = ConnectionFactory.getFactory().getConnection();
        this.conn.setAutoCommit(true);
        this.conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      } else {
        this.conn = conn;
        isConnInner = false;
      }
      this.schTName    = schTName;
      this.eodpopdTmp  = eodpopdTmp;
      this.eodpodeals  = eodpodeals;
      this.accntabTmp  = accntabTmp;
      this.pod         = pod;
      this.whereSQL    = whereSQL;
      this.module      = module;
      this.cPRB        = cPRB;
      /** ���������������� java.sql.Statement */
      java.sql.Statement st = this.conn.createStatement();
      ResultSet rss = null;
      /**  ����������� ������ ��������, �������� �������� � ���� ��������� ������� TMPEODPOPD<module> */
      try {
        st.execute("DROP TABLE " + eodpopdTmp + module);
      } catch (Exception ex) {
//        logger.error("��� ������� ������� ������� " + eodpopdTmp + module + " - " + ex.getMessage());
      }
      st.execute("CREATE TABLE " + eodpopdTmp + module + " LIKE " + eodpopdTmp);
//      logger.info("������� " + eodpopdTmp + module + " �������");
      rqR = st.executeUpdate("INSERT INTO " + eodpopdTmp + module + " SELECT * FROM " + eodpopdTmp + " P WHERE " + this.whereSQL);
      /** ������� ������� �� ������� <TMPEODPOPD> */
      crtIndxsForEodpopd(st, eodpopdTmp + module);
//      logger.info("����������� �� ��������� ������� " + eodpopdTmp + module + " - " + rqR + " �������");

      /**  ����������� ������ ������ �� ����� ������ � ���� ��������� ������� TMPEODPD<module> */
      dealsJoin = isUsingDeals(module);
      if (dealsJoin != null) {
        try {
          st.execute("DROP TABLE " + eodpodeals + module);
        } catch (Exception ex) {
//          logger.error("��� ������� ������� ������� " + eodpodeals + module + " - " + ex.getMessage());
        }
        st.execute("CREATE TABLE " + eodpodeals + module + " LIKE " + eodpodeals);
//        logger.info("������� " + eodpodeals + module + " �������");
        rqDR = st.executeUpdate("INSERT INTO " + eodpodeals + module + " SELECT * FROM " + eodpodeals + " DEALS " +
          "WHERE EXISTS (SELECT * FROM " + eodpopdTmp + module + " P WHERE " + dealsJoin + ")");
        /** ������� ������� �� ������� <TMPEODPD> */
        crtIndxsForEodpd(st, eodpodeals + module);
//        logger.info("����������� �� ��������� ������� " + eodpodeals + module + " - " + rqDR + " �������");
      }

      /** �������� ���-�� ������������, ������� ����� ����������� ������� ������� */
//      System.out.println(this.whereSQL);
//      ResultSet rss = st.executeQuery("SELECT COUNT(*) FROM " + eodpopdTmp + " P WHERE " + this.whereSQL);
//      if (rss.next())
//        cnt = rss.getInt(1);
//      rss.close();
      /** �������� ����������� ��������, �� �������� ����� ����� �������� ���-�� �������� */
      rss = st.executeQuery("SELECT NEXT VALUE FOR " + schemaDWH + ".PDLOAD_SEQ FROM SYSIBM.SYSDUMMY1 ");
      if (rss.next())
        minID = rss.getLong(1);
      rss.close();
      /** ������ ��������� �������� �������� � ������ ���-�� ����������� �������� */
      st.execute("ALTER SEQUENCE " + schemaDWH + ".PDLOAD_SEQ RESTART WITH " + (rqR + minID));
      st.close();
      logger.info("Started " + module + " - " + rqR + " (min ID=" + minID + ",deals=" + rqDR + ")");
    }

    public void run() {
      int totalInserted = 0; // ����� ���-�� ����������� �������
      long begTime = System.currentTimeMillis();
      int[] rqs = new int[4]; // ��� LE (���-�� ������������ ������� � ������ ������)
      try {
//        String sql = getSQL(pod, schTName, eodpopdTmp, eodpodeals, accntabTmp, whereSQL, module, minID);
        String sql = null;
        java.sql.Statement st = conn.createStatement();
        if (module.startsWith("LE")) {
          // ��� LE - ����� SQL �� 4 ����� - �.�. ������� �� ����������� INSERT with SELECT - ��������
          for (int i=0; i<4; i++) {
            sql = getSQL(pod, schTName, eodpopdTmp+module, eodpodeals+module, accntabTmp, null, module, minID + totalInserted, i);
            int rq = st.executeUpdate(sql);
//            logger.info(module + ", sql " + i + " - " + rq);
            rqs[i] = rq;
            totalInserted += rq;
          }
        } else {
          sql = getSQL(pod, schTName, eodpopdTmp+module, eodpodeals+module, accntabTmp, null, module, minID, 0);
          totalInserted = st.executeUpdate(sql);
//          logger.info(module + ", sql " + 0 + " - " + rq);
        }
        st.execute("DROP TABLE " + eodpopdTmp + module);
        if (dealsJoin != null)
          st.execute("DROP TABLE " + eodpodeals + module);
        st.close();
        if (isConnInner) {
          conn.close();
          conn = null;
        }
      } catch (Exception ex) {
        error = "������ ��� ���������� INSERT ������� �� " + eodpopdTmp + module + " � " + schTName + " �� ������ " + module + "; " +
            cPRB.getErrTxt(ex);
        ex.printStackTrace();
        try {
          conn.rollback();
          if (isConnInner) {
            conn.close();
            conn = null;
          }
        } catch (Exception ex1) {
          ex1.printStackTrace();
        }
      }
      long durTime = Math.round((System.currentTimeMillis() - begTime) / 1000d);
      String finStr = module.startsWith("LE") ? rqs[0] + "," + rqs[1] + "," + rqs[2] + "," + rqs[3] : totalInserted + "";
      logger.info("Finished " + module + " - inserted=" + totalInserted + "(" + finStr + "); time=" + durTime + " s)");
      finished = true;
    }
  }

  /**
   * ���������� �������� ������������ ������ ������ - GetPostings, ���������� ������ � PDLOAD - ����� SavePostings,
   * ����� ���������� ���������� ������������ � ������ ������. <BR>
   * @param pod ����, �� ������� ������������ �������; <BR>
   * @param schTName ��� ����� � ������� PDLOAD ���� ��������� ������������; <BR>
   * @param eodpopdTmp �����+������������ ��������� ������� EODPOPD, ������ ��������� ������ � PDLOAD; <BR>
   * @param eodpopdSrc �����+������������ �������� ������� EODPOPD, ������ ��������� ������ MIDAS; <BR>
   * @param pbrBatchsForNotLoad ������ PBR-Batch��, ������� �� ����� ���������; <BR>
   * @param eodpodeals ��������� ������� ������ ������ ��� �������� � PDLOAD; <BR>
   * @param accntabTmp ��������� ������� ������ MIDAS; <BR>
   * @param pbrs ������������ ������� ����� �������, ������� ����������; <BR>
   * @return true - ����������� �������, false - �� �����������; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private boolean startProcess(java.sql.Date pod, String schTName, String eodpopdTmp, String eodpopdSrc, String pbrBatchsForNotLoad,
        String eodpodeals, String accntabTmp, String pbrs) throws Exception {
    if (pbrBatchsForNotLoad != null && !pbrBatchsForNotLoad.equals(""))
      pbrBatchsForNotLoad = "P.BATCHPBR NOT IN (" + pbrBatchsForNotLoad + ")";
//        pbrBatchsForNotLoad = "AND P.BATCHPBR NOT IN ('ZZZ')";
    GenerateWhereSQLForParts genWhere = new GenerateWhereSQLForParts();
    Object res = null;
    int arLen = 0;
    String[][] batchs = new String[0][0], iers = new String[0][0], ics = new String[0][0], les = new String[0][0];
    if (pbrs == null || pbrs.indexOf(Constants.BATCH) != -1) {
       res    = genWhere.getArFilterXXX(null, pbrBatchsForNotLoad, eodpopdTmp, Constants.BATCH, Constants.BATCH_BAT, genWhere.REGIM_LOAD, pod);
       batchs = (String[][])res;
       arLen  += batchs.length;
    }
    if (pbrs == null || pbrs.indexOf(Constants.GE_IER) != -1) {
      res   = genWhere.getArFilterXXX(null, null, eodpopdTmp, Constants.GE_IER, Constants.GE_IER_IER, genWhere.REGIM_LOAD, pod);
      iers  = (String[][])res;
      arLen += iers.length;
    }
    if (pbrs == null || pbrs.indexOf(Constants.GE_IC) != -1) {
      res   = genWhere.getArFilterXXX(null, null, eodpopdTmp, Constants.GE_IC, Constants.GE_IC_IC, genWhere.REGIM_LOAD, pod);
      ics   = (String[][])res;
      arLen += ics.length;
    }
    if (pbrs == null || pbrs.indexOf(Constants.GE_LE) != -1) {
      res   = genWhere.getArFilterXXX(null, null, eodpopdTmp, Constants.GE_LE, Constants.GE_LE_LE, genWhere.REGIM_LOAD, pod);
      les   = (String[][])res;
      arLen += les.length;
    }
    if (pbrs == null || pbrs.indexOf(Constants.GE_FT) != -1)
      arLen++;
    if (pbrs == null || pbrs.indexOf(Constants.GE_ST) != -1)
      arLen++;
    if (pbrs == null || pbrs.indexOf(Constants.GE_CE) != -1)
      arLen++;
    if (pbrs == null || pbrs.indexOf(Constants.GE_DL) != -1)
      arLen++;
    if (pbrs == null || pbrs.indexOf("OTHER") != -1)
      arLen++; // OTHER
    String[][] modules = new String[arLen][];

    int ind = 0;
    for (int i = 0; i<batchs.length; i++)
      modules[ind++] = new String[]{"BATCH_" + i, batchs[i][0]};
    for (int i = 0; i<iers.length; i++)
      modules[ind++] = new String[]{"IER_" + i, iers[i][0]};
    for (int i = 0; i<ics.length; i++)
      modules[ind++] = new String[]{"IC_" + i, ics[i][0]};
    for (int i = 0; i<les.length; i++)
      modules[ind++] = new String[]{"LE_" + i, les[i][0]};
    if (pbrs == null || pbrs.indexOf(Constants.GE_FT) != -1)
      modules[ind++] = new String[]{"FT",      "P.SPOS='GE-FT'"};
    if (pbrs == null || pbrs.indexOf(Constants.GE_ST) != -1)
      modules[ind++] = new String[]{"ST",      "P.SPOS='GE-ST'"};
    if (pbrs == null || pbrs.indexOf(Constants.GE_CE) != -1)
      modules[ind++] = new String[]{"CE",      "P.SPOS='GE-CE'"};
    if (pbrs == null || pbrs.indexOf("OTHER") != -1)
      modules[ind++] = new String[]{"OTHER",   "P.PBRN<>'BATCH' AND P.SPOS NOT IN ('GE-DL','GE-LE','GE-IER','GE-CE','GE-IC','GE-FT','GE-ST')"};
    if (pbrs == null || pbrs.indexOf(Constants.GE_DL) != -1)
      modules[ind++] = new String[]{"DL",      "P.SPOS='GE-DL'"};

    try {

      SavePostings[] sp = new SavePostings[modules.length];
      for (int i=0; i<modules.length; i++) {
        sp[i] = new SavePostings(null, schTName, eodpopdTmp, pod, modules[i][1], modules[i][0], eodpodeals, accntabTmp, this);
        sp[i].start();
//        Thread.currentThread().sleep(2000); // ������� ��������� ������ ����������� ������ - ���������, ����� �����?
      }
      while (true) { // ������� ��������� ���������� ���� �������
        boolean isFinished = true;
        for (int i=0; i<modules.length; i++) {
          isFinished = isFinished & sp[i].finished;
        }
        if (isFinished)
          break;
        Thread.currentThread().sleep(1000); // ������� 1 ��� � ��������� ��������� �����
      }
      boolean isError = false;
      for (int i=0; i<modules.length; i++) {
        if (sp[i].error != null) {
          logger.error(modules[i][0] + ": " + sp[i].error);
          isError = true;
        }
      }
      if (isError)
        return false;


    } catch (Exception ex) {
      logger.error("������ ��� ������� �������� ������ � " + schTName + " �� " + eodpopdTmp, ex);
      try {
        connection.rollback();
      } catch (Exception ex1) {
        ex1.printStackTrace();
      }
    }
    return true;
  }

  /**
   * ��������� ������� ������ �� ������� �� ������� ��������� � PDLOAD. <BR>
   * @param conn java.sql.Connection ��� ������ � ��; <BR>
   * @param wrkDay ������� ����; <BR>
   * @param eodpopdTmp ����� + ��������� ������� EODPOPD, ����� ������� �������� ������ � PDLOAD; <BR>
   * @param eodpodealsTmp ����� + ��������� ������� EODPODEALS, ����� ������� �������� ������ �� ������� � PDLOAD; <BR>
   * @return ������� ������ �� �������: true - ������� �������, false - �� �������; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private boolean gatherDeals(java.sql.Connection conn, java.sql.Date wrkDay, String eodpopdTmp, String eodpodealsTmp) throws Exception {
//    lv.gcpartners.bank.util.CallCommandOnAS400 prgAS400 = new lv.gcpartners.bank.util.CallCommandOnAS400();
//    String cmd1 = "CLRPFM FILE(" + schemaDWH.toUpperCase() + "/EODPODEALS)";
//    if (!prgAS400.call(cmd1, null, null)) {
//      throw new Exception ("������� " + schemaDWH.toUpperCase() + "/EODPODEALS �� ���� �������");
//    }
//    logger.info("�������� ������� " + schemaDWH.toUpperCase() + "/EODPODEALS");
//    prgAS400 = null;
    java.sql.Statement st = conn.createStatement();

    try {
      st.execute("DROP TABLE " + eodpodealsTmp);
    } catch (SQLException ex) {
    }
    try {
      st.execute("CREATE TABLE " + eodpodealsTmp + " (FXSC CHAR(3) CCSID 1025 DEFAULT NULL, FXPC CHAR(3) CCSID 1025 DEFAULT NULL, " +
        "FXSA DECIMAL(19, 0) DEFAULT NULL, FXPA DECIMAL(19, 0) DEFAULT NULL, FXBC CHAR(3) CCSID 1025 DEFAULT NULL, " +
        "FTYP CHAR(3) CCSID 1025 DEFAULT NULL, FSEQ CHAR(2) CCSID 1025 DEFAULT NULL, DLVDAT DATE DEFAULT NULL, " +
        "DLMDAT DATE DEFAULT NULL, DLID INTEGER DEFAULT NULL, DLACSQ CHAR(2) CCSID 1025 DEFAULT NULL, DLCNUM CHAR(6) CCSID 1025 DEFAULT NULL, " +
        "FCNUM CHAR(6) CCSID 1025 DEFAULT NULL, ORIG_BORR CHAR(6) CCSID 1025 DEFAULT NULL, DLNO_ORIG CHAR(6) CCSID 1025 DEFAULT NULL, " +
        "DLNO CHAR(6) CCSID 1025 DEFAULT NULL, DLID_ORIG INTEGER DEFAULT NULL, DLORED DATE DEFAULT NULL, " +
        "DLVALUE DATE DEFAULT NULL, RECI CHAR(1) CCSID 1025 DEFAULT NULL, DL CHAR(1) CCSID 1025 DEFAULT NULL, " +
        "OTRF CHAR(15) CCSID 1025 DEFAULT NULL, CNUMCE_DB CHAR(6) CCSID 1025 DEFAULT NULL, CNUMCE_CR CHAR(6) CCSID 1025 DEFAULT NULL, " +
        "OTST CHAR(10) CCSID 1025 DEFAULT NULL, DLMATUR DATE, CCY CHAR(3)) " +
        "");
    } catch (SQLException ex) {
      logger.error("������ ��� ������� ������� ������� " + eodpodealsTmp, ex);
      return false;
    }

    st.execute("CREATE INDEX " + eodpodealsTmp + "_IDX0 ON " + eodpodealsTmp + " (DLID)");
    logger.info("����������� ������� " + eodpodealsTmp + " (������ " + eodpodealsTmp + "_IDX0) ");

//    st.execute("DECLARE GLOBAL TEMPORARY TABLE " +  eodpodealsTmp + " LIKE " + schemaDWH + ".EODPODEALS WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS");
//    st.execute("CREATE INDEX " + eodpodealsTmp + "_IDX ON " + eodpodealsTmp + " (DLNO, DL)");

    /***********************************************/
    /** ��������� EODPODEALS �� ������� �� LOANTAB */
    /***********************************************/
    st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.LE_DLIDS (DLID INTEGER) WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
    int rq = st.executeUpdate("INSERT INTO SESSION.LE_DLIDS " +
      "SELECT DISTINCT D.ID FROM " + eodpopdTmp + " P JOIN " + schemaDWH + ".DELOTAB D ON P.DLNO=D.DLNO AND D.DL='L' " +
      "WHERE P.DL='L' AND '" + wrkDay + "' BETWEEN D.DAT AND D.DATTO ");
    st.execute("CREATE UNIQUE INDEX SESSION.LE_DLIDS_IDX ON SESSION.LE_DLIDS (DLID) ");
    logger.info("������� ��������� ������� SESSION.LE_DLIDS, � �� ����������� " + rq + " ��������� ������ �� " + eodpopdTmp);

    /** ������� ������� ��� ���� ����� DLVALUE, DLMATUR � SESSION.LE_DLIDS1, ����� � ����� ����� ������ � eodpodealsTmp */
    st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.LE_DLIDS1 LIKE " + eodpodealsTmp + " WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
//    rq = st.executeUpdate("INSERT INTO " + eodpodealsTmp + " " +
    rq = st.executeUpdate("INSERT INTO SESSION.LE_DLIDS1 " +
      "(fxsc, fxpc, FXBC, FTYP, FSEQ, DLVDAT, DLMDAT, DLID, DLCNUM, FCNUM, ORIG_BORR, DLNO_ORIG, DLNO, DLID_ORIG, DLORED, RECI, DL, CCY) " +
      "select DELOTAB.CCY as fXsc, DELOTAB.CCY as fXpc, LOANTAB.BRCA as fXbc, loantab.ftyp, loantab.fseq, loantab.vdat as dlvdat, " +
      "loantab.mdat as dlmdat, delotab.id as dlid, loantab.cnum as dlcnum, loantab.fcus as fcnum, loantab.olno as orig_borr, " +

//      "delotab.dlno as dlno_ORIG, " +
      "(select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=delotab.origid) as dlno_orig, " +

      "delotab.dlno as dlno, " +
      "delotab.origid as dlid_orig, loantab.ored AS DLORED, " +

//      "(select LOANtab2.vdat from " + schemaDWH + ".delotab delotab2, " + schemaDWH + ".loantab loantab2 " +
//      "where delotab2.id=delotab.origid and loantab2.loanid=delotab.origid and delotab2.dat=loantab2.dat) as dlvalue, " +

      "delotab.reci, DELOTAB.DL AS DL, DELOTAB.CCY " +

//      "(select LOANtab2.MDAT FROM " + schemaDWH + ".delotab delotab2, " + schemaDWH + ".loantab loantab2 " +
//      "where delotab2.id=delotab.origid and loantab2.loanid=delotab.origid and delotab2.dat=loantab2.dat) as DLMATUR " +

      "from " + schemaDWH + ".delotab delotab " +
      "left join " + schemaDWH + ".loantab loantab on delotab.id=loantab.loanid and loantab.datto=delotab.datto " +

      "where delotab.id in (select DLID from SESSION.LE_DLIDS) ");
    logger.info("� ������� SESSION.LE_DLIDS1 ���� ��������� " + rq + " ������� �� LE �������");

//    rq = st.executeUpdate("UPDATE " + eodpodealsTmp + " EPD SET " +
//      "DLVALUE=(select L2.vdat from " + schemaDWH + ".delotab d2, " + schemaDWH + ".loantab l2 " +
//      "where d2.id=EPD.DLID_ORIG and l2.loanid=d2.id and d2.dat=l2.dat), " +
//      "DLMATUR=(select L2.MDAT FROM " + schemaDWH + ".delotab d2, " + schemaDWH + ".loantab l2 " +
//      "where d2.id=EPD.DLID_ORIG and l2.loanid=d2.id and d2.dat=l2.dat) " +
//      "where EPD.DLID in (select DLID from SESSION.LE_DLIDS) ");
//    logger.info("���������� DLVALUE, DLMATUR � " + eodpodealsTmp + " �� SESSION.LE_DLIDS ");
    rq = st.executeUpdate("INSERT INTO " + eodpodealsTmp + " " +
      "(fxsc, fxpc, FXBC, FTYP, FSEQ, DLVDAT, DLMDAT, DLID, DLCNUM, FCNUM, ORIG_BORR, DLNO_ORIG, DLNO, DLID_ORIG, " +
      "DLORED, RECI, DL, CCY, DLVALUE, DLMATUR) " +
      "SELECT fxsc, fxpc, FXBC, FTYP, FSEQ, DLVDAT, DLMDAT, DLID, DLCNUM, FCNUM, ORIG_BORR, DLNO_ORIG, DLNO, DLID_ORIG, " +
      "DLORED, RECI, DL, CCY, " +
      "(select L2.vdat from " + schemaDWH + ".delotab d2, " + schemaDWH + ".loantab l2 where d2.id=EPD.DLID_ORIG and " +
      "l2.loanid=d2.id and d2.dat=l2.dat) as dlvalue, " +
      "(select L2.MDAT FROM " + schemaDWH + ".delotab d2, " + schemaDWH + ".loantab l2 where d2.id=EPD.DLID_ORIG and " +
      "l2.loanid=d2.id and d2.dat=l2.dat) as DLMATUR " +
      "FROM SESSION.LE_DLIDS1 EPD ");
    logger.info("� " + eodpodealsTmp + " �� SESSION.LE_DLIDS1 ���� ��������� " + rq + " ������� �� LE �������");

    conn.commit();
    st.execute("DROP TABLE SESSION.LE_DLIDS1 ");
    st.execute("DROP TABLE SESSION.LE_DLIDS  ");
    conn.commit();
    if (rq == 0) // ������ LE - ������ ����
      return false;

    /**********************************************/
    /** ��������� EODPODEALS �� ������� �� DEXTAB */
    /**********************************************/
    st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.RLND_FX (DTYP CHAR(2), DLST CHAR(2)) WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
    st.executeUpdate("INSERT INTO SESSION.RLND_FX SELECT DISTINCT LTYP, SUTP FROM " + schemaDWH + ".GCPRLNDEAL " +
      "WHERE '" + wrkDay + "' BETWEEN DAT AND DATTO AND DL='O' " +
      "");
    rq = st.executeUpdate("insert into " + eodpodealsTmp + " " +
      "(fxsc, fxpc, fxsa, fxpa, fxbc, dlvdat, dlmdat, dlid, dlcnum, dlno_orig, dlno, dlid_orig, dlvalue, reci, dl) " +
      "select dextab.pucy as fXsc, dextab.slcy as fXpc, dextab.puam as fXsa, dextab.slam as FXpa, " +
      "dextab.brca as fXbc, dextab.ddat as dlvdat, " +

      "(select CASE WHEN G.DTYP IS NOT NULL THEN DX2.OTDT ELSE DX2.VDAT END " +
      "from " + schemaDWH + ".delotab d2, " + schemaDWH + ".dextab dx2 where d2.id=DELOTAB.ORIGID and " +
      "DX2.dealid=d2.id and d2.dat=dx2.dat) AS DLMDAT, " +
//      (curVer >= 156 ? "(CASE WHEN G.DTYP IS NOT NULL THEN DEXTAB.OTDT ELSE dextab.vdat END) as dlmdat, " : "DEXTAB.VDAT AS DLMDAT, ") +

      "delotab.id as dlid, dextab.cnum as dlcnum, " +
      "(select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=delotab.origid) as dlno_orig, delotab.dlno as dlno, " +
      "delotab.origid as dlid_orig, " +

      "(select DX2.DDAT from " + schemaDWH + ".delotab d2, " + schemaDWH + ".dextab dx2 where d2.id=DELOTAB.ORIGID and " +
      "DX2.dealid=d2.id and d2.dat=dx2.dat) AS DLVALUE, " +
//      "dextab.ddat as dlvalue, " +

      "delotab.reci, DELOTAB.DL as dl " +
      "from " + schemaDWH + ".delotab delotab " +
      "left join " + schemaDWH + ".dextab dextab on delotab.id=dextab.dealid and delotab.datto=dextab.datto " +
      "LEFT JOIN SESSION.RLND_FX G ON G.DTYP=DEXTAB.DTYP AND G.DLST=DEXTAB.DLST " +
      "where '" + wrkDay + "' between delotab.dat and delotab.datto and delotab.dl='E' and " +
      "exists (select * from " + eodpopdTmp + " e where e.dlno=delotab.dlno and e.DL='E') ");

    st.execute("DROP TABLE SESSION.RLND_FX ");
    conn.commit();
    logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FX �������");
    if (rq == 0) // ������ FX - ������ ����
      return false;

    /***********************************************/
    /** ��������� EODPODEALS �� ������� �� DEALTAB */
    /***********************************************/
    st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.DL_DLIDS (DLID INTEGER) WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
    rq = st.executeUpdate("INSERT INTO SESSION.DL_DLIDS " +
      "SELECT DISTINCT D.ID FROM " + eodpopdTmp + " P JOIN " + schemaDWH + ".DELOTAB D ON P.DLNO=D.DLNO AND D.DL='D' " +
      "WHERE P.DL='D' AND '" + wrkDay + "' BETWEEN D.DAT AND D.DATTO ");
    st.execute("CREATE UNIQUE INDEX SESSION.DL_DLIDS_IDX ON SESSION.DL_DLIDS (DLID) ");
    logger.info("������� ��������� ������� SESSION.DL_DLIDS, � �� ����������� " + rq + " ��������� ������ �� " + eodpopdTmp);

    /** ������� ������� ��� ���� ����� DLVALUE, DLMATUR � SESSION.DL_DLIDS1, ����� � ����� ����� ������ � eodpodealsTmp */
    st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.DL_DLIDS1 LIKE " + eodpodealsTmp + " WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
    st.execute("ALTER TABLE SESSION.DL_DLIDS1 ADD COLUMN ORIGDAT DATE ");
//    rq = st.executeUpdate("insert into " + eodpodealsTmp + " " +
    rq = st.executeUpdate("INSERT INTO SESSION.DL_DLIDS1 " +
      "(fxsc, fxpc, fxsa, fxpa, fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, reci, dl, ORIGDAT) " +

      "select DELOTAB.CCY as fXsc, DELOTAB.CCY as fXpc, DEALTAB.RODA as fXsa, DEALTAB.RBDA as FXpa, DEALTAB.BRCA as fXbc, " +
      "dealtab.vdat as dlvdat, dealtab.mdat as dlmdat, delotab.id as dlid, dealtab.cdas as DLACSQ, dealtab.cnum as dlcnum, " +
      "(select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=delotab.origid) as dlno_orig, delotab.dlno as dlno, " +
      "delotab.origid as dlid_orig, dealtab.ored DLORED, " +

//      "(select dealtab2.vdat from " + schemaDWH + ".delotab delotab2, " + schemaDWH + ".dealtab dealtab2 " +
//      "where delotab2.id=delotab.origid and dealtab2.dealid=delotab.origid and delotab2.dat=dealtab2.dat) as dlvalue, " +
      "delotab.reci, DELOTAB.DL as dl, " +
//      "(select dealtab2.MDAT from " + schemaDWH + ".delotab delotab2, " + schemaDWH + ".dealtab dealtab2 " +
//      "where delotab2.id=delotab.origid and dealtab2.dealid=delotab.origid and delotab2.dat=dealtab2.dat) as DLMATUR " +
      "(SELECT DAT FROM " + schemaDWH + ".DELOTAB WHERE ID=DELOTAB.ORIGID) AS ORIGDAT " +

      "from " + schemaDWH + ".delotab delotab " +
      "left join " + schemaDWH + ".dealtab dealtab on delotab.id=dealtab.dealid and dealtab.datto=delotab.datto " +

      "where delotab.ID IN (select DLID from SESSION.DL_DLIDS)");
    st.execute("CREATE INDEX SESSION.DL_DLIDS1_IDX ON SESSION.DL_DLIDS1 (ORIGDAT, DLID_ORIG) ");
    logger.info("� ������� SESSION.DL_DLIDS1 ���� ��������� " + rq + " ������� �� MM �������");

//    rq = st.executeUpdate("UPDATE " + eodpodealsTmp + " EPD SET " +
//      "DLVALUE=(select dt2.vdat from " + schemaDWH + ".delotab d2, " + schemaDWH + ".dealtab dt2 " +
//      "where d2.id=EPD.DLID_ORIG and dt2.dealid=d2.id and d2.dat=dt2.dat), " +
//      "DLMATUR=(select dt2.MDAT from " + schemaDWH + ".delotab d2, " + schemaDWH + ".dealtab dt2 " +
//      "where d2.id=EPD.DLID_ORIG and dt2.dealid=d2.id and d2.dat=dt2.dat) " +
//      "where EPD.DLID in (select DLID from SESSION.DL_DLIDS) ");
//    logger.info("���������� DLVALUE, DLMATUR � " + eodpodealsTmp + " �� SESSION.DL_DLIDS ");

//    rq = st.executeUpdate("INSERT INTO " + eodpodealsTmp + " " +
//      "(fxsc, fxpc, fxsa, fxpa, fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, reci, dl, DLVALUE, DLMATUR) " +
//      "SELECT fxsc, fxpc, fxsa, fxpa, fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, reci, dl, " +
//      "(select dt2.vdat from " + schemaDWH + ".dealtab dt2 where dt2.dealid=EPD.DLID_ORIG and dt2.dat=EPD.ORIGDAT) AS DLVALUE, " +
//      "(select dt2.MDAT from " + schemaDWH + ".dealtab dt2 where dt2.dealid=EPD.DLID_ORIG and dt2.dat=EPD.ORIGDAT) AS DLMATUR " +
//      "FROM SESSION.DL_DLIDS1 EPD ");
//    logger.info("� " + eodpodealsTmp + " �� SESSION.DL_DLIDS1 ���� ��������� " + rq + " ������� �� MM �������");

    st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.DLID_ORIGS_MM (DLID INTEGER, VDAT DATE, MDAT DATE) " +
      "WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
    int rqMM = st.executeUpdate("INSERT INTO SESSION.DLID_ORIGS_MM (DLID, VDAT, MDAT) " +
      "SELECT D.DEALID, D.VDAT, D.MDAT FROM " + schemaDWH + ".DEALTAB D WHERE (DEALID, DAT) IN " +
      "(SELECT S.DLID_ORIG, S.ORIGDAT FROM SESSION.DL_DLIDS1 S) ");
    logger.info("�� ��������� ������� SESSION.DLID_ORIGS_MM ���� ��������� " + rqMM + " �������");
    rqMM = st.executeUpdate("UPDATE SESSION.DL_DLIDS1 S SET (DLVALUE, DLMATUR)=" +
      "(SELECT VDAT, MDAT FROM SESSION.DLID_ORIGS_MM D WHERE D.DLID=S.DLID_ORIG)");
    logger.info("�� ��������� ������� SESSION.DL_DLIDS1 ���� ��������� " + rqMM + " �������");
    st.execute("DROP TABLE SESSION.DLID_ORIGS_MM");

    rq = st.executeUpdate("INSERT INTO " + eodpodealsTmp + " " +
      "(fxsc, fxpc, fxsa, fxpa, fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, reci, dl, DLVALUE, DLMATUR) " +
      "SELECT fxsc, fxpc, fxsa, fxpa, fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, reci, dl, " +
      "DLVALUE, DLMATUR FROM SESSION.DL_DLIDS1 EPD ");
    logger.info("� " + eodpodealsTmp + " �� SESSION.DL_DLIDS1 ���� ��������� " + rq + " ������� �� MM �������");

    conn.commit();
    st.execute("DROP TABLE SESSION.DL_DLIDS1 ");
    st.execute("DROP TABLE SESSION.DL_DLIDS  ");
    conn.commit();
    if (rq == 0) // ������ MM - ������ ����
      return false;

    if (this.curVer >= 158) {
      /**********************************************/
      /** ��������� EODPODEALS �� ������� �� FIMTAB */
      /**********************************************/
      //Falko 2014-09-17 ���������� FXSC, FXPC, FXSA, FXPA
    	rq = st.executeUpdate("insert into " + eodpodealsTmp + " " +
          "(fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, dlvalue, reci, dl, FXSC, FXPC, FXSA, FXPA) " +
          "select F.BRCA as fXbc, " +
          "F.vdat as dlvdat, F.mdat as dlmdat, D.id as dlid, F.cdas as DLACSQ, F.cnum as dlcnum, " +
          "(select d2.dlno from " + schemaDWH + ".delotab d2 where d2.id=D.origid) as dlno_orig, D.dlno as dlno, " +
          "D.origid as dlid_orig, F.ored DLORED, (select F2.vdat from " + schemaDWH + ".delotab d2, " + schemaDWH + ".FIMtab F2 " +
          "where d2.id=D.origid and F2.FIMid=D.origid and d2.dat=F2.dat) as dlvalue, " +
          "D.reci, D.DL as dl, " +
          "case when ACKEY_3 in ('V','D') then UCUCY when ACKEY_3 in ('M') then TCUCY else null end as FXSC, " +
          "case when ACKEY_3 in ('V','D') then TCUCY when ACKEY_3 in ('M') then UCUCY else null end as FXPC, " +
          "case when ACKEY_3 in ('V','D') then UPAMT when ACKEY_3 in ('M') then TPAMT else null end as FXSA, " +
          "case when ACKEY_3 in ('V','D') then TPAMT when ACKEY_3 in ('M') then UPAMT else null end as FXPA " +
          "from " + schemaDWH + ".delotab D " +
          "left join " + schemaDWH + ".FIMtab F on D.id=F.FIMid and D.datto=F.datto " +
          "left join (select dlno, max(SUBSTR(TRIM(ACKEY),3,1)) as ACKEY_3 from " + eodpopdTmp + " where DL='I' group by dlno) e on e.dlno=D.dlno " +
          "where '" + wrkDay + "' between D.dat and D.datto and D.dl='I' and " +
          "exists (select * from " + eodpopdTmp + " e where e.dlno=D.dlno and e.DL='I')");

    	/*���� �� 2014-09-17
	      rq = st.executeUpdate("insert into " + eodpodealsTmp + " " +
	        "(fxbc, dlvdat, dlmdat, dlid, dlacsq, dlcnum, dlno_orig, dlno, dlid_orig, dlored, dlvalue, reci, dl) " +
	        "select F.BRCA as fXbc, " +
	        "F.vdat as dlvdat, F.mdat as dlmdat, D.id as dlid, F.cdas as DLACSQ, F.cnum as dlcnum, " +
	        "(select d2.dlno from " + schemaDWH + ".delotab d2 where d2.id=D.origid) as dlno_orig, D.dlno as dlno, " +
	        "D.origid as dlid_orig, F.ored DLORED, (select F2.vdat from " + schemaDWH + ".delotab d2, " + schemaDWH + ".FIMtab F2 " +
	        "where d2.id=D.origid and F2.FIMid=D.origid and d2.dat=F2.dat) as dlvalue, " +
	        "D.reci, D.DL as dl " +
	        "from " + schemaDWH + ".delotab D " +
	        "left join " + schemaDWH + ".FIMtab F on D.id=F.FIMid and D.datto=F.datto " +
	        "where '" + wrkDay + "' between D.dat and D.datto and D.dl='I' and " +
	        "exists (select * from " + eodpopdTmp + " e where e.dlno=D.dlno and e.DL='I')");
      */
      conn.commit();
      logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FRA/IRS ������� ");
    }

    if (this.curVer >= 155) {
      /*******************************************************/
      /** ��������� EODPODEALS �� ������� �� FEETAB (FELOAN) */
      /*******************************************************/
      rq = st.executeUpdate("insert into " + eodpodealsTmp + " " +
        "(fxbc, dlvdat, dlmdat, dlid, dlcnum, dlored, dlvalue, reci, dl, ftyp, fseq, dlno, FCNUM, OTRF, CCY) " +
        "select F.FEBRCA as fXbc, F.FEPSTD as dlvdat, F.FEPEND as dlmdat, D.id as dlid, F.FECNUM as dlcnum, " +
        "F.FEORED DLORED, F.FEPSTD as dlvalue, D.reci, D.DL as dl, FT.FACT, FT.FCNO, " +
        "D1.DLNO, " + // DLNO ������ LE - ������ � TMPEODPOPD
        "F.FECNUM, DIGITS(F.FEFCOD), F.FEFCCY " + // ������ fee ���, �.�.���� � F � D
        "from " + schemaDWH + ".delotab D " +
        "left join " + schemaDWH + ".FEEtab F on D.id=F.FEEid and '" + wrkDay + "' BETWEEN F.DAT AND F.DATTO AND " +
          "F.FEFSEQ=(SELECT MAX(FEFSEQ) FROM " + schemaDWH + ".FEETAB WHERE FEEID=F.FEEID AND FEFCCY=F.FEFCCY " +
          "AND '" + wrkDay + "' BETWEEN DAT AND DATTO) AND " +
          "F.FEEID=(SELECT MAX(FEEID) FROM " + schemaDWH + ".FEETAB WHERE FELOAN=F.FELOAN AND FEFCCY=F.FEFCCY AND FECNUM=F.FECNUM AND " +
          "FEFCOD=F.FEFCOD AND FEFACT=F.FEFACT AND FEFCNO=F.FEFCNO AND '" + wrkDay + "' BETWEEN DAT AND DATTO) " +
        "LEFT JOIN " + schemaDWH + ".FCLTAB FT ON D.FCLID=FT.FCLID AND '" + wrkDay + "' BETWEEN FT.DAT AND FT.DATTO " +
        "LEFT JOIN " + schemaDWH + ".FEEHEADER FH ON FH.FEEID=F.FEEID AND FH.FEESEQ=F.FEFSEQ AND FH.FCLID=D.FCLID AND '" + wrkDay + "' BETWEEN FH.DAT AND FH.DATTO " +
        "LEFT JOIN " + schemaDWH + ".DELOTAB D1 ON D1.ID=FH.LOANID AND '" + wrkDay + "' BETWEEN D1.DAT AND D1.DATTO " +
        "where '" + wrkDay + "' between D.dat and D.datto and D.dl='C' and " +
        "exists (" +
        "SELECT * " +
        "FROM " + eodpopdTmp + " E " +
        "JOIN " + schemaDWH + ".DELOTAB D ON D.DLNO=E.DLNO AND D.DL='L' AND '" + wrkDay + "' BETWEEN D.DAT AND D.DATTO " +
        "JOIN " + schemaDWH + ".FEEHEADER FH ON FH.LOANID=D.ID AND D.FCLID=FH.FCLID AND '" + wrkDay + "' BETWEEN FH.DAT AND FH.DATTO " +
        "WHERE F.FEEID=FH.FEEID AND E.DL='C' AND E.OTST<>'' " +
        ") " +
        "");
      conn.commit();
      logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FEE ������� (FELOAN)");

      /*******************************************************/
      /** ��������� EODPODEALS �� ������� �� FEETAB (FEFACL) */
      /*******************************************************/
      rq = st.executeUpdate("insert into " + eodpodealsTmp + " " +
        "(fxbc, dlvdat, dlmdat, dlid, dlcnum, dlored, dlvalue, reci, dl, ftyp, fseq, dlno, FCNUM, OTRF, CCY) " +
        "select F.FEBRCA as fXbc, F.FEPSTD as dlvdat, F.FEPEND as dlmdat, D.id as dlid, F.FECNUM as dlcnum, " +
        "F.FEORED DLORED, F.FEPSTD as dlvalue, D.reci, D.DL as dl, FT.FACT, FT.FCNO, " +
        (curVer >= 161 ? "' '" : "'0'") + "||F.FEFACT||F.FEFCNO DLNO, " +
        "F.FECNUM, DIGITS(F.FEFCOD), F.FEFCCY " + // ������ fee ���, �.�.���� � F � D
        "from " + schemaDWH + ".delotab D " +
        "left join " + schemaDWH + ".FEEtab F on D.id=F.FEEid and '" + wrkDay + "' BETWEEN F.DAT AND F.DATTO AND " +
          "F.FEFSEQ=(SELECT MAX(FEFSEQ) FROM " + schemaDWH + ".FEETAB WHERE FEEID=F.FEEID AND FEFCCY=F.FEFCCY " +
          "AND '" + wrkDay + "' BETWEEN DAT AND DATTO) AND " +
          "F.FEEID=(SELECT MAX(FEEID) FROM " + schemaDWH + ".FEETAB WHERE FELOAN=F.FELOAN AND FEFCCY=F.FEFCCY AND FECNUM=F.FECNUM AND " +
          "FEFCOD=F.FEFCOD AND FEFACT=F.FEFACT AND FEFCNO=F.FEFCNO AND '" + wrkDay + "' BETWEEN DAT AND DATTO) " +
        "LEFT JOIN " + schemaDWH + ".FCLTAB FT ON D.FCLID=FT.FCLID AND '" + wrkDay + "' BETWEEN FT.DAT AND FT.DATTO " +
        "where '" + wrkDay + "' between D.dat and D.datto and D.dl='C' and " +
        "exists (select * from " + eodpopdTmp + " e where e.OTTP=F.FEFACT AND RIGHT(E.DLNO, 2)=F.FEFCNO AND E.DL='C' AND E.OTST='') ");
      conn.commit();
      logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FEE ������� (FEFACL)");
    }


    /** ��������� EODPODEALS �� ������� CE */
    rq = st.executeUpdate(
      "insert into " + eodpodealsTmp + " (fxsc, fxpc, fxsa, fxpa, fxbc, CNUMCE_DB, CNUMCE_CR, OTRF) " +
      "select cu.drcy as fXsc, cu.crcy as fXpc, cu.dram as fXsa, cu.cram as FXpa, cu.brca as fXbc, " +
      "cu.dcus, cu.ccus, IPDN " +
      "from " + schemaDWHIN + ".CUSEXCE cu WHERE RECI IN ('D','C')");
    conn.commit();
    logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� CE �������");

    /** ��������� EODPODEALS �� ������� FT (IN) */
    rq = st.executeUpdate("insert into " + eodpodealsTmp + " (fxsc, fxpc, fxsa, fxpa, fxbc, OTRF) " +
      "select I.SMCY as fXsc, I.PCCY as fXpc, I.SMAM as fXsa, I.PYAM as FXpa, I.brca as fXbc, I.PREF " +
      "from " + schemaDWHIN + ".INPAYDD I ");
    conn.commit();
    logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FT(I) �������");

    /** ��������� EODPODEALS �� ������� FT (OUT) */
    rq = st.executeUpdate("insert into " + eodpodealsTmp + " (fxsc, fxpc, fxsa, fxpa, fxbc, OTRF) " +
      "select O.SMCY as fXsc, O.PCCY as fXpc, O.SMAM as fXsa, O.PYAM as FXpa, O.brca as fXbc, O.PREF " +
      "from " + schemaDWHIN + ".OTPAYDD O ");
    conn.commit();
    logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FT(O) �������");

    /** ��������� EODPODEALS �� ������� FT (TRANSF) */
    rq = st.executeUpdate("insert into " + eodpodealsTmp + " (fxsc, fxpc, fxsa, fxpa, fxbc, OTRF) " +
      "select N.CCY as fXsc, N.CCY as fXpc, N.AMNT as fXsa, N.AMNT as FXpa, N.brca as fXbc, N.TFRF " +
      "from " + schemaDWHIN + ".NTRANDD N ");
    conn.commit();
    logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� FT(N) �������");

    /** ��������� EODPODEALS �� ������� ST */
    rq = st.executeUpdate("insert into " + eodpodealsTmp + " (DLVDAT, DLMDAT, DLVALUE, OTST) " +
      "select DISTINCT date(s.itld+719892) as DLVDAT, date(s.maty+719892) as DLMDAT, date(s.itld+719892) as DLVALUE, S.SESN " +
      "from " + schemaDWHIN + ".SECTYD S ");
    conn.commit();
    logger.info("� " + eodpodealsTmp + " ���� ��������� " + rq + " ������� �� ST �������");

    /** ������� ������� �� ������� EODPODEALS */
    crtIndxsForEodpd(st, eodpodealsTmp);

    st.close();
    return true;
  }

  /**
   * ����������� ������ �� TMPP2BLST � PDLOADDOC. <BR>
   * @param conn java.sql.Connection ��� ������ � ��; <BR>
   * @param wrkDay ������� ����; <BR>
   * @param pdloadVar ����� � ��� ������� PDLOAD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void cpyP2BLST(java.sql.Connection conn, java.sql.Date wrkDay, String pdloadVar) throws Exception {
    java.sql.Statement st = conn.createStatement();
    int rq = st.executeUpdate("INSERT INTO " + schemaDWH + ".PDLOADDOC (ID, PNARR, OPERTYPE, DOCNUM, DOCDATE) " +
      "SELECT ID, DETAILS, OPERTYPE, DOCNUM, DOCDATE FROM " + schemaDWH + ".TMPP2BLST T " +
      "WHERE NOT EXISTS (SELECT * FROM " + schemaDWH + ".PDLOADDOC WHERE ID=T.ID) ");
    logger.info("��������� " + rq + " ������� � " + schemaDWH + ".PDLOADDOC �� " + schemaDWH + ".TMPP2BLST");

    /** ��������� �������� � ���� AVISTP */
    try {
      st.execute("DECLARE GLOBAL TEMPORARY TABLE SESSION.IDS (ID BIGINT) WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ");
      java.sql.Statement st1 = conn.createStatement();
      int totalCnt = 0;

      // �� ������������ ��������� ����
      ResultSet rss = st1.executeQuery("SELECT TRIM(VALUE_STR), TRIM(CODE) FROM " + schemaDWH + ".PARAMS WHERE " +
        "CODE LIKE 'MIO_TRNAME%' AND '" + wrkDay + "' BETWEEN DAT AND DATTO ");
      while (rss.next()) {
        rq = st.executeUpdate("INSERT INTO SESSION.IDS SELECT ID_DOC FROM " + pdloadVar + " WHERE POD='" + wrkDay + "' AND STATUS='I' AND " +
          "TRAT IN (SELECT A1RTTY FROM " + schemaDWH + ".SDRETRPD WHERE A1RTNM LIKE '" + rss.getString(1) + "%')");
        totalCnt += rq;
        logger.info("�� TRNAME " + rss.getString(2) + " �������� � SESSION.IDS " + rq + " �������� ");
      }
      rss.close();

      // �� ���������
      rss = st1.executeQuery("SELECT TRIM(VALUE_STR), TRIM(CODE) FROM " + schemaDWH + ".PARAMS WHERE " +
        "CODE LIKE 'MIO_PNAR%' AND '" + wrkDay + "' BETWEEN DAT AND DATTO ");
      while (rss.next()) {
        rq = st.executeUpdate("INSERT INTO SESSION.IDS SELECT ID_DOC FROM " + pdloadVar + " WHERE POD='" + wrkDay + "' AND STATUS='I' AND " +
          "PNAR LIKE '" + rss.getString(1) + "%' ");
        totalCnt += rq;
        logger.info("�� PNAR " + rss.getString(2) + " �������� � SESSION.IDS " + rq + " �������� ");
      }
      rss.close();

      // �� ���� ��������� ����
      rss = st1.executeQuery("SELECT TRIM(VALUE_STR), TRIM(CODE) FROM " + schemaDWH + ".PARAMS WHERE " +
        "CODE LIKE 'MIO_TRCODE%' AND '" + wrkDay + "' BETWEEN DAT AND DATTO ");
      while (rss.next()) {
        rq = st.executeUpdate("INSERT INTO SESSION.IDS SELECT ID_DOC FROM " + pdloadVar + " WHERE POD='" + wrkDay + "' AND STATUS='I' AND " +
          "TRAT IN ('" + rss.getString(1) + "')");
        totalCnt += rq;
        logger.info("�� TRCODE " + rss.getString(2) + " �������� � SESSION.IDS " + rq + " �������� ");
      }
      rss.close();

      // ������� ������� ���.������
      String avistp = "";
      rss = st1.executeQuery("SELECT TRIM(VALUE_STR), TRIM(CODE) FROM " + schemaDWH + ".PARAMS WHERE " +
        "CODE = 'MIO_AVISTP' AND '" + wrkDay + "' BETWEEN DAT AND DATTO ");
      if (rss.next())
        avistp = rss.getString(1);
      rss.close();

      st1.close();

      if (totalCnt > 0) {
        rq = st.executeUpdate("UPDATE " + schemaDWH + ".PDLOADDOC SET AVISTP='" + avistp + "' WHERE ID IN (SELECT ID FROM SESSION.IDS) ");
        logger.info("�������� " + rq + " ������� � " + schemaDWH + ".PDLOADDOC (AVISTP=" + avistp + ") ");
      }

    } catch (Exception ex) {
      logger.error("������ ��� ������������ ���� AVISTP", ex);
    }
    st.close();
  }

  /**
   * �������� � PDLOAD/PDLOADDOC �� TMPP2BLST �������������� PNARR. <BR>
   * @param wrkDay ������� ����; <BR>
   * @param pdloadVar ����� + ������� PDLOAD; <BR>
   * @throws Exception ��������� ������. <BR>
   */
  private void repPDLOAD_PNARR(java.sql.Date wrkDay, String pdloadVar) throws Exception {
    int rq = 0;
    java.sql.Statement st = connection.createStatement();
    PDLOADSCH = pdloadVar;

    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET ID_DOC=(SELECT ID FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and " +
      "p2b.acod=p.acod and p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and " +
      "(CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and p2b.ASOC=p.asoc and p2b.TRAT=p.trat and " +
      "p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.DLREF=p.dlTYPE||P.OTRF and " +
      "p2b.OTTP=p.ottp and p2b.OTST=p.otst and p2b.BOKC=p.bokc) " +
      "WHERE POD='" + wrkDay + "' AND STATUS='I' AND PBR='GE-DL' AND EXISTS (SELECT * FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod " +
      "and p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr " +
      "and p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and " +
      "p2b.DLREF=p.dlTYPE||P.OTRF and p2b.OTTP=p.ottp and p2b.OTST=p.otst and p2b.BOKC=p.bokc) ");
    logger.info("���-�� �������� ID_DOC � " + PDLOADSCH + " " + rq + " (DL)");

    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET ID_DOC=(SELECT ID FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and " +
      "p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and " +
      "p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.OTTP=p.ottp " +
      "and p2b.OTST=p.otst and p2b.BOKC=p.bokc) " +
      "WHERE POD='" + wrkDay + "' AND STATUS='I' AND BATCHID>0 AND EXISTS (SELECT * FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and " +
      "p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and " +
      "p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.OTTP=p.ottp " +
      "and p2b.OTST=p.otst and p2b.BOKC=p.bokc) ");
    logger.info("���-�� �������� ID_DOC � " + PDLOADSCH + " " + rq + " (BATCH)");

    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET ID_DOC=(SELECT ID FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and " +
      "p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and " +
      "p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.OTTP=p.ottp " +
      "and p2b.OTST=p.otst and p2b.BOKC=p.bokc) " +
      "WHERE POD='" + wrkDay + "' AND STATUS='I' AND PBR='GE-FT' AND EXISTS (SELECT * FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and " +
      "p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and " +
      "p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.OTTP=p.ottp " +
      "and p2b.OTST=p.otst and p2b.BOKC=p.bokc) ");
    logger.info("���-�� �������� ID_DOC � " + PDLOADSCH + " " + rq + " (FT)");

    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET ID_DOC=(SELECT ID FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and " +
      "p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and " +
      "p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.OTTP=p.ottp " +
      "and p2b.OTST=p.otst and p2b.BOKC=p.bokc) " +
      "WHERE POD='" + wrkDay + "' AND STATUS='I' AND PBR='GE-CE' AND EXISTS (SELECT * FROM " + schemaDWH + ".TMPP2BLST P2B WHERE " +
      "p2b.pstd=p.pod and p2b.vald=p.vald and p2b.spos=p.PBR and p2b.CNUM=p.cnum and p2b.CCY=p.ccy and p2b.acod=p.acod and " +
      "p2b.acsq=p.acsq and p2b.BRCA=p.brca and p2b.PSTA=p.AMNT and (CASE WHEN p2b.DRCR=1 THEN 'C' ELSE 'D' END)=P.drcr and " +
      "p2b.ASOC=p.asoc and p2b.TRAT=p.trat and p2b.PNAR=p.pnar and p2b.OTRF=p.otrf and p2b.DPMT=p.dpmt and p2b.OTTP=p.ottp " +
      "and p2b.OTST=p.otst and p2b.BOKC=p.bokc) ");
    logger.info("���-�� �������� ID_DOC � " + PDLOADSCH + " " + rq + " (CE)");

    cpyP2BLST(connection, wrkDay, PDLOADSCH);

    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET PNARR=(SELECT PNARR FROM " + schemaDWH + ".PDLOADDOC WHERE P.ID_DOC=ID) " +
      "WHERE P.POD='" + wrkDay + "' AND P.ID_DOC IS NOT NULL AND P.ID_DOC>0 ");
    logger.info("���-�� �������� PNARR � " + PDLOADSCH + " �� " + schemaDWH + ".PDLOADDOC " + rq + " (DL,BATCH,FT,CE)");
    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET (ID_DOC, PNARR)=(SELECT ID_DOC, PNARR FROM " + PDLOADSCH + " " +
      "WHERE P.PDRF=ID) WHERE P.POD='" + wrkDay + "' AND PDRF IN (SELECT ID FROM " + PDLOADSCH + " WHERE POD='" + wrkDay + "' " +
      "AND ID_DOC IS NOT NULL AND STATUS='I' AND ID_DOC>0) AND PDRF>0 AND PBR<>'@@RCA' ");
    logger.info("���-�� �������� ID_DOC, PNARR � " + PDLOADSCH + " �� PDRF " + rq + " (DL,BATCH,FT,CE)");
    rq = st.executeUpdate("UPDATE " + PDLOADSCH + " P SET (ID_DOC, PNARR)=(SELECT ID_DOC, PNARR FROM " + PDLOADSCH + " " +
      "WHERE P.OPID=OPID AND STATUS='I' AND POD='" + wrkDay + "' AND OPID>0 AND P.DRCR=DRCR) " +
      "WHERE P.POD='" + wrkDay + "' AND STATUS<>'I' AND OPID IN (SELECT OPID FROM " + PDLOADSCH + " " +
      "WHERE POD='" + wrkDay + "' AND ID_DOC IS NOT NULL AND ID_DOC>0 AND STATUS='I' AND OPID>0) AND OPID>0 AND PBR<>'@@RCA' ");
    logger.info("���-�� �������� ID_DOC, PNARR � " + PDLOADSCH + " �� OPID " + rq + " (DL,BATCH,FT,CE)");

    st.close();
  }

  /**
   * �������� SQL ��� �������� ��������� ����� � PDLOAD �� ������ ������ � ��������� ���� (DL-MM). <BR>
   * @param wrkDay ���� ����.���; <BR>
   * @param tblName �����+��� ������� � ������� ��������� ������; <BR>
   * @return SQL ��� �������� ��������� �����. <BR>
   */
  public String getSQLForFillingDealFieldsMM(java.sql.Date wrkDay, String tblName) {
    String sql =
      "update " + tblName + " p set " +
      "FXSC=coalesce(fxsc, (select d.Ccy from " + schemaDWH + ".delotab d WHERE d.dl='D' and p.otrf=d.dlno and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXPC=coalesce(fxpc, (select d.Ccy from " + schemaDWH + ".delotab d WHERE d.dl='D' and p.otrf=d.dlno and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXSA=coalesce(fxsa, (select deALtab.RODA from " + schemaDWH + ".delotab d join " + schemaDWH + ".deALtab deALtab on " +
        "deALtab.dealid=d.id and d.dl='D' and p.otrf=d.dlno and  d.datto=deALtab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXPA=coalesce(fxpa, (select deALtab.RBDA from " + schemaDWH + ".delotab d join " + schemaDWH + ".deALtab DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXBC=coalesce(fxbc, (select DEALTAB.brca from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLVDAT=coalesce(dlvdat, (select DEALTAB.Vdat from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLMDAT=coalesce(dlmdat, (select DEALTAB.Mdat from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLCNUM=coalesce(dlcnum, (select DEALTAB.cnum from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLID=coalesce(dlid, (select d.id from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLNO_ORIG=coalesce(dlno_orig, (select (select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=d.origid) " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on DEALTAB.dealid=d.id and d.dl='D' and " +
        "p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLNO=coalesce(dlno, (select d.dlno from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLID_ORIG=coalesce(case when dlid_orig=-1 then null else dlid_orig end, (select d.origid " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DEALTAB on " +
        "DEALTAB.dealid=d.id and d.dl='D' and p.otrf=d.dlno and d.datto=DEALTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +

      "dlvalue=coalesce(dlvalue, (SELECT (select dl2.vdat from " + schemaDWH + ".delotab d2 join " + schemaDWH + ".dealtab dl2 on " +
        "dl2.dealid=d2.id and dl2.datto=d2.datto where d2.id=d.origid) " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DL on DL.dealid=d.id and d.dl='D' and p.otrf=d.dlno and " +
        "d.datto=DL.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlmatur=coalesce(dlmatur, (SELECT (select dl2.mdat from " + schemaDWH + ".delotab d2 join " + schemaDWH + ".dealtab dl2 on " +
        "dl2.dealid=d2.id and dl2.datto=d2.datto where d2.id=d.origid) " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".DEALTAB DL on DL.dealid=d.id and d.dl='D' and p.otrf=d.dlno and " +
        "d.datto=DL.datto and '" + wrkDay + "' between d.dat and d.datto)) " +

      "where p.pod='" + wrkDay + "' and LEFT(P.OTRF,6)=? AND P.PBR='GE-DL' and " +
      "p.ottp in (select ottp from " + schemaDWH + ".gcp_mdtype where pbr='GE-DL' and module IN ('MM')) ";
    return sql;
  }

  /**
   * �������� SQL ��� �������� ��������� ����� � PDLOAD �� ������ ������ � ��������� ���� (DL-FI). <BR>
   * @param wrkDay ���� ����.���; <BR>
   * @param tblName �����+��� ������� � ������� ��������� ������; <BR>
   * @return SQL ��� �������� ��������� �����. <BR>
   */
  public String getSQLForFillingDealFieldsFI(java.sql.Date wrkDay, String tblName) {
    String sql =
      "update " + tblName + " p set " +
//Falko 2014-09-17
      "FXSC = coalesce(FXSC, (select case " +
  			"when SUBSTR(TRIM(ACKEY),3,1) in ('V','D') then UCUCY " + 
	  		"when SUBSTR(TRIM(ACKEY),3,1) in ('M') then TCUCY else null end " + 
			  "from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " + 
	  	  "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), "+
      "FXPC = coalesce(FXPC, (select case " +
  			"when SUBSTR(TRIM(ACKEY),3,1) in ('V','D') then TCUCY " + 
	  		"when SUBSTR(TRIM(ACKEY),3,1) in ('M') then UCUCY else null end " + 
			  "from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " + 
		    "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), "+
	    "FXSA = coalesce(FXSA, (select case " +
  			"when SUBSTR(TRIM(ACKEY),3,1) in ('V','D') then UPAMT " + 
	  		"when SUBSTR(TRIM(ACKEY),3,1) in ('M') then TPAMT else null end " + 
			  "from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " + 
		  	"FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), "+
	    "FXPA = coalesce(FXPA, (select case " +
  			"when SUBSTR(TRIM(ACKEY),3,1) in ('V','D') then TPAMT " + 
	  		"when SUBSTR(TRIM(ACKEY),3,1) in ('M') then UPAMT else null end " + 
			  "from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " + 
		  	"FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), "+
/*���� �� 2014-09-17

      "FXSC=coalesce(fxsc, (select d.Ccy from " + schemaDWH + ".delotab d WHERE d.dl='I' and p.otrf=d.dlno and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXPC=coalesce(fxpc, (select d.Ccy from " + schemaDWH + ".delotab d WHERE d.dl='I' and p.otrf=d.dlno and '" + wrkDay + "' between d.dat and d.datto)), " +
*/
      "FXBC=coalesce(fxbc, (select FIMTAB.brca from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLVDAT=coalesce(dlvdat, (select FIMTAB.Vdat from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLMDAT=coalesce(dlmdat, (select FIMTAB.Mdat from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLCNUM=coalesce(dlcnum, (select FIMTAB.cnum from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLID=coalesce(dlid, (select d.id from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLNO_ORIG=coalesce(dlno_orig, (select (select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=d.origid) " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on FIMTAB.FIMID=d.id and d.DL='I' and " +
        "p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLNO=coalesce(dlno, (select d.dlno from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLID_ORIG=coalesce(case when dlid_orig IN (-1,0) then null else dlid_orig end, (select d.origid " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".FIMTAB FIMTAB on " +
        "FIMTAB.FIMID=d.id and d.DL='I' and p.otrf=d.dlno and d.datto=FIMTAB.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlvalue=coalesce(dlvalue, (select dl2.vdat from " + schemaDWH + ".DELOTAB D JOIN " + schemaDWH + ".delotab d2 ON D.ORIGID=D2.ID " +
        "join " + schemaDWH + ".FIMTAB dl2 on dl2.FIMID=d2.id and dl2.datto=d2.datto " +
        "where D.DL='I' and p.otrf=d.dlno and '" + wrkDay + "' between d.dat and d.datto)), " +
      "DLMARK=coalesce(DLMARK, (select F.BYSL from " + schemaDWH + ".FIMTAB F WHERE p.otrf=F.dlno and '" + wrkDay + "' between F.dat and F.datto)) " +
      "where p.pod='" + wrkDay.toString() + "' and LEFT(P.OTRF,6)=? AND P.PBR='GE-DL' and " +
      "p.ottp in (select ottp from " + schemaDWH + ".gcp_mdtype where pbr='GE-DL' and module IN ('FI')) ";
    return sql;
  }

  public void repairFillingsDataDealsInPDLOAD(java.sql.Date wrkDay, String pbr) throws Exception {
    java.sql.Statement st = connection.createStatement();

    st.executeUpdate("update RUBARS01.PDLOAD p set " +
      "FXSC=(SELECT FXSC FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "FXPC=(SELECT FXPC FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "FXSA=(SELECT FXSA FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "FXPA=(SELECT FXPA FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "FXBC=(SELECT FXBC FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLVDAT=(SELECT DLVDAT FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLMDAT=(SELECT DLMDAT FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLCNUM=(SELECT DLCNUM FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLID=(SELECT DLID FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLNO_ORIG=(SELECT DLNO_ORIG FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLNO=(SELECT DLNO FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLID_ORIG=(SELECT DLID_ORIG FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLACSQ=(SELECT DLACSQ FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "DLORED=(SELECT DLORED FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D'), " +
      "dlvalue=(SELECT DLVALUE FROM RUBARS01.EODPODEALS WHERE P.OTRF=DLNO AND DL='D') " +

      "where p.pod='" + wrkDay.toString() + "' and P.PBR='GE-DL' and P.DLID IS NULL ");

    st.executeUpdate(
      "update RUBARS01.PDLOAD p set " +
      "FXSC=     (SELECT DISTINCT(FXSC)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FXPC=     (SELECT DISTINCT(FXPC)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FXSA=     (SELECT DISTINCT(FXSA)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FXPA=     (SELECT DISTINCT(FXPA)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FXBC=     (SELECT DISTINCT(FXBC)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FTYP=     (SELECT DISTINCT(FTYP)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FSEQ=     (SELECT DISTINCT(FSEQ)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLVDAT=   (SELECT DISTINCT(DLVDAT)    FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLMDAT=   (SELECT DISTINCT(DLMDAT)    FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLCNUM=   (SELECT DISTINCT(DLCNUM)    FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "FCNUM=    (SELECT DISTINCT(FCNUM)     FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "ORIG_BORR=(SELECT DISTINCT(ORIG_BORR) FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLID=     (SELECT DISTINCT(DLID)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLNO_ORIG=(SELECT DISTINCT(DLNO_ORIG) FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLNO=     (SELECT DISTINCT(DLNO)      FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLID_ORIG=(SELECT DISTINCT(DLID_ORIG) FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "DLORED=   (SELECT DISTINCT(DLORED)    FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L'), " +
      "dlvalue=  (SELECT DISTINCT(DLVALUE)   FROM RUBARS01.EODPODEALS E1 JOIN RUBARS01.EODPOPD E2 ON E1.DLNO=E2.DLNO WHERE P.OTRF=E2.OTRF AND E1.DL='L') " +

      "where p.pod='" + wrkDay.toString() + "' and P.PBR='GE-LE' and P.DLID IS NULL "
      );

    st.close();

//update RUBARS01.PDLOAD p set
//FXSC=(SELECT FXSC FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXPC=(SELECT FXPC FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXSA=(SELECT FXSA FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXPA=(SELECT FXPA FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXBC=(SELECT FXBC FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//CNUMCE_DB=(SELECT CNUMCE_DB FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//CNUMCE_CR=(SELECT CNUMCE_CR FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF)
//
//where p.pod='09.10.2008' and P.PBR='GE-CE' AND P.FXSC IS NULL;

//update RUBARS01.PDLOAD p set
//FXSC=(SELECT FXSC FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXPC=(SELECT FXPC FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXSA=(SELECT FXSA FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXPA=(SELECT FXPA FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF),
//FXBC=(SELECT FXBC FROM RUBARS01.EODPODEALS WHERE P.OTRF=OTRF)
//
//where p.pod='09.10.2008' and P.PBR='GE-FT' AND P.FXSC IS NULL;


//    update RUBARS01.PDLOAD p set
//    DLVDAT=(SELECT DLVDAT FROM RUBARS01.EODPODEALS WHERE P.OTST=OTST),
//    DLMDAT=(SELECT DLMDAT FROM RUBARS01.EODPODEALS WHERE P.OTST=OTST),
//    DLVALUE=(SELECT DLVALUE FROM RUBARS01.EODPODEALS WHERE P.OTST=OTST)
//
//    where p.pod='09.10.2008' and P.PBR='GE-ST' AND P.DLVDAT IS NULL;

  }

  /**
   * �������� SQL ��� �������� ��������� ����� � PDLOAD �� ������ ������ � ��������� ���� (DL-FX). <BR>
   * @param wrkDay ���� ����.���; <BR>
   * @return SQL ��� �������� ��������� �����. <BR>
   */
  public String getSQLForFillingDealFieldsFX(java.sql.Date wrkDay) {
    String sql =
      "update " + schemaDWH + "." + PDLOAD + " p set " +
      "FXSC=COALESCE(FXSC, (select dextab.pucy from " + schemaDWH + ".delotab d join rubars01.dextab dextab on " +
          "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXPC=COALESCE(FXPC, (select dextab.slcy from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
          "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "FXSA=COALESCE(FXSA, (select dextab.puam from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
          "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "fxpa=COALESCE(FXPA, (select dextab.slam from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
          "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "fxbc=COALESCE(FXBC, (select dextab.brca from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
          "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlvdat=COALESCE(DLVDAT, (select dextab.ddat from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
        "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlmdat=COALESCE(DLMDAT, (select DISTINCT CASE WHEN G.LTYP IS NOT NULL THEN DX2.OTDT ELSE DX2.VDAT END " +
        "from " + schemaDWH + ".DELOTAB D JOIN " + schemaDWH + ".delotab d2 ON D.ORIGID=D2.ID " +
        "JOIN " + schemaDWH + ".dextab dx2 ON DX2.dealid=d2.id and d2.dat=dx2.dat " +
        "LEFT JOIN " + schemaDWH + ".GCPRLNDEAL G ON G.LTYP=DX2.DTYP AND G.SUTP=DX2.DLST AND '" + wrkDay + "' BETWEEN G.DAT AND G.DATTO AND G.DL='O' " +
        "WHERE P.OTRF=D.DLNO AND D.DL='E' AND '" + wrkDay + "' BETWEEN D.DAT AND D.DATTO)), " +
      "dlid=COALESCE(DLID, (select d.id from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
        "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlcnum=COALESCE(DLCNUM, (select dextab.cnum from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
        "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlno_orig=COALESCE(DLNO_ORIG, (select (select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=d.origid) " +
        "from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
        "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlno=COALESCE(DLNO, (select d.dlno from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
        "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlid_orig=COALESCE(CASE WHEN DLID_ORIG=-1 THEN NULL ELSE DLID_ORIG END, " +
        "(select d.origid from " + schemaDWH + ".delotab d join " + schemaDWH + ".dextab dextab on " +
        "dextab.dealid=d.id and d.dl='E' and p.otrf=d.dlno and d.datto=dextab.datto and '" + wrkDay + "' between d.dat and d.datto)), " +
      "dlvalue=COALESCE(DLVALUE, (select DX2.DDAT from " + schemaDWH + ".DELOTAB D JOIN " + schemaDWH + ".delotab d2 ON D.ORIGID=D2.ID " +
        "JOIN " + schemaDWH + ".dextab dx2 ON DX2.dealid=d2.id and d2.dat=dx2.dat " +
        "WHERE P.OTRF=D.DLNO AND D.DL='E' AND '" + wrkDay + "' BETWEEN D.DAT AND D.DATTO)) " +
      "where p.pod='" + wrkDay + "' and LEFT(P.OTRF,6)=? AND P.PBR='GE-DL' and " +
          "p.ottp in (select ottp from " + schemaDWH + ".gcp_mdtype where pbr='GE-DL' and module='FX') ";
//      logger.info(sql);
    return sql;
  }

  /**
   * �������� SQL ��� �������� ��������� ����� � PDLOAD �� ������ ������ � ��������� ���� (LE). <BR>
   * @param wrkDay ���� ����.���; <BR>
   * @return SQL ��� �������� ��������� �����. <BR>
   */
  public String[] getSQLForFillingDealFieldsLE(java.sql.Date wrkDay) {
    String sql1 = "DECLARE GLOBAL TEMPORARY TABLE SESSION.DEALS " +
     "(FXSC CHAR(3), FXPC CHAR(3), FXBC CHAR(3), FTYP CHAR(3), FSEQ CHAR(2), DLVDAT DATE, DLMDAT DATE, DLID INTEGER, " +
     "DLCNUM CHAR(6), FCNUM CHAR(6), ORIG_BORR CHAR(6), DLNO_ORIG CHAR(6), DLNO CHAR(6), DLID_ORIG INTEGER, DLORED DATE, DLVALUE DATE) " +
     "WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ";
    String sql2 = "INSERT INTO SESSION.DEALS " +
      "select DELOTAB.CCY as fXsc, DELOTAB.CCY as fXpc, LOANTAB.BRCA as fXbc, loantab.ftyp, loantab.fseq, loantab.vdat as dlvdat, " +
      "loantab.mdat as dlmdat, delotab.id as dlid, loantab.cnum as dlcnum, loantab.fcus as fcnum, loantab.olno as orig_borr, " +
      "(select dl2.dlno from " + schemaDWH + ".delotab dl2 where dl2.id=delotab.origid) as dlno_orig, delotab.dlno as dlno, " +
      "delotab.origid as dlid_orig, loantab.ored AS DLORED, " +
      "(select LOANtab2.vdat from " + schemaDWH + ".delotab delotab2, " + schemaDWH + ".loantab loantab2 " +
      "where delotab2.id=delotab.origid and loantab2.loanid=delotab.origid and delotab2.dat=loantab2.dat) as dlvalue " +
//      "loantab.vdat as dlvalue " +
      "from " + schemaDWH + ".delotab delotab left join " + schemaDWH + ".loantab loantab on " +
      "delotab.id=loantab.loanid and loantab.datto=delotab.datto " +
      "where '" + wrkDay.toString() + "' between delotab.dat and delotab.datto and delotab.dl='L' and DELOTAB.DLNO=?";
    String sql3 = "update " + schemaDWH + "." + PDLOAD + " p set " +
      "FXSC=     COALESCE(FXSC,      (select d.FXSC      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "FXPC=     COALESCE(FXPC,      (select d.FXPC      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "fxbc=     COALESCE(FXBC,      (select d.fxbc      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "ftyp=     COALESCE(FTYP,      (select d.ftyp      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "fseq=     COALESCE(FSEQ,      (select d.fseq      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlvdat=   COALESCE(DLVDAT,    (select d.dlvdat    from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlmdat=   COALESCE(DLMDAT,    (select d.dlmdat    from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlid=     COALESCE(DLID,      (select d.dlid      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlcnum=   COALESCE(DLCNUM,    (select d.dlcnum    from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "fcnum=    COALESCE(FCNUM,     (select d.fcnum     from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "orig_borr=COALESCE(ORIG_BORR, (select d.orig_borr from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlno_orig=COALESCE(DLNO_ORIG, (select d.dlno_orig from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlno=     COALESCE(DLNO,      (select d.dlno      from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlid_orig=COALESCE((CASE WHEN DLID_ORIG=-1 THEN NULL ELSE DLID_ORIG END), " +
      "(select d.dlid_orig from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlored=   COALESCE(DLORED,  (select d.dlored    from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno)), " +
      "dlvalue=  COALESCE(DLVALUE, (select d.dlvalue   from SESSION.DEALS d WHERE left(p.otrf,6)=d.dlno))  " +
      "where p.pod='" + wrkDay.toString() + "' and LEFT(P.OTRF,6)=? AND P.PBR='GE-LE' ";
    String sql4 = "DELETE FROM SESSION.DEALS";
    return new String[]{sql1, sql2, sql3, sql4};
  }

  /**
   * �������� SQL ��� �������� ��������� ����� � PDLOAD �� ������ ������ � ��������� ���� (FE). <BR>
   * @param wrkDay ���� ����.���; <BR>
   * @param pdloadids �� ������������ �� PDLOAD; <BR>
   * @return SQL ��� �������� ��������� �����. <BR>
   */
  public String[] getSQLForFillingDealFieldsFE(java.sql.Date wrkDay, String pdloadids) {
    String sql1 = "DECLARE GLOBAL TEMPORARY TABLE SESSION.DEALS (" +
     "FXBC CHAR(3), DLVDAT DATE, DLMDAT DATE, DLID INTEGER, DLCNUM CHAR(6), DLORED DATE, DLVALUE DATE, RECI CHAR(1), DL CHAR(1), " +
     "FTYP CHAR(3), FSEQ CHAR(2), DLNO CHAR(6), FCNUM CHAR(6), OTRF CHAR(2), CCY CHAR(3), ID BIGINT) " +
     "WITH REPLACE NOT LOGGED ON COMMIT PRESERVE ROWS ";

// "(fxbc, dlvdat, dlmdat, dlid, dlcnum, dlored, dlvalue, reci, dl, ftyp, fseq, dlno, FCNUM, OTRF, CCY) " +
// "select F.FEBRCA as fXbc, F.FEPSTD as dlvdat, F.FEPEND as dlmdat, D.id as dlid, F.FECNUM as dlcnum, " +
// "F.FEORED DLORED, F.FEPSTD as dlvalue, D.reci, D.DL as dl, FT.FACT, FT.FCNO, " +
// "D1.DLNO, " + // DLNO ������ LE - ������ � TMPEODPOPD
// "F.FECNUM, DIGITS(F.FEFCOD), F.FEFCCY " + // ������ fee ���, �.�.���� � F � D

// "(fxbc, dlvdat, dlmdat, dlid, dlcnum, dlored, dlvalue, reci, dl, ftyp, fseq, dlno, FCNUM, OTRF, CCY) " +
// "select F.FEBRCA as fXbc, F.FEPSTD as dlvdat, F.FEPEND as dlmdat, D.id as dlid, F.FECNUM as dlcnum, " +
// "F.FEORED DLORED, F.FEPSTD as dlvalue, D.reci, D.DL as dl, FT.FACT, FT.FCNO, " +
// "'0'||F.FEFACT||F.FEFCNO DLNO, " +
// "F.FECNUM, DIGITS(F.FEFCOD), F.FEFCCY " + // ������ fee ���, �.�.���� � F � D

    String sql2 =
      "INSERT INTO SESSION.DEALS (fxbc, dlvdat, dlmdat, dlid, dlcnum, dlored, dlvalue, reci, dl, ftyp, fseq, dlno, FCNUM, OTRF, CCY, ID) " +
      "select F.FEBRCA as fXbc, F.FEPSTD as dlvdat, F.FEPEND as dlmdat, F.FEEID as dlid, F.FECNUM as dlcnum, F.FEORED DLORED, " +
      "F.FEPSTD as dlvalue, D.RECI, D.DL AS DL, FT.FACT, FT.FCNO, D1.DLNO, F.FECNUM, DIGITS(F.FEFCOD), F.FEFCCY, P.ID " +

      "FROM " + schemaDWH + "." + PDLOAD + " P " +
      "JOIN " + schemaDWH + ".FEETAB F ON LEFT(P.OTRF, 6)=F.FELOAN AND P.ASOC=F.FECNUM AND " +
        "RIGHT(P.OTRF, 3)=TRIM(CHAR(F.FEFCOD))||RIGHT(P.OTRF, 1) AND P.CCY=F.FEFCCY AND '" + wrkDay + "' BETWEEN F.DAT AND F.DATTO AND " +
        "F.FEEID=(SELECT MAX(FEEID) FROM " + schemaDWH + ".FEETAB WHERE F.FELOAN=FELOAN AND F.FECNUM=FECNUM AND F.FEFCOD=FEFCOD AND " +
        "F.FEFCCY=FEFCCY AND '" + wrkDay + "' BETWEEN DAT AND DATTO) " +
      "JOIN " + schemaDWH + ".DELOTAB D ON F.FEEID=D.ID " +
      "JOIN " + schemaDWH + ".FCLTAB FT ON FT.FCLID=D.FCLID " +
      "JOIN " + schemaDWH + ".DELOTAB D1 ON F.FELOAN=D1.DLNO AND D1.DL='L' AND '" + wrkDay + "' BETWEEN D1.DAT AND D1.DATTO " +

      "where P.ID IN (" + pdloadids + ") AND P.OTST<>'' AND P.STATUS IN ('I','M') AND P.PDID=0 AND P.DLTYPE='C' " +
      " ";

    String sql3 =
      "INSERT INTO SESSION.DEALS (fxbc, dlvdat, dlmdat, dlid, dlcnum, dlored, dlvalue, reci, dl, ftyp, fseq, dlno, FCNUM, OTRF, CCY, ID) " +
      "select F.FEBRCA as fXbc, F.FEPSTD as dlvdat, F.FEPEND as dlmdat, F.FEEID as dlid, F.FECNUM as dlcnum, F.FEORED DLORED, " +
      "F.FEPSTD as dlvalue, D.RECI, D.DL AS DL, F.FEFACT, F.FEFCNO, " + (curVer >= 161 ? "' '" : "'0'") + "||F.FEFACT||F.FEFCNO, F.FECNUM, " +
      "DIGITS(F.FEFCOD), F.FEFCCY, P.ID " +

      "FROM " + schemaDWH + "." + PDLOAD + " P " +
      "JOIN " + schemaDWH + ".FEETAB F ON '000000'=F.FELOAN AND P.ASOC=F.FECNUM AND " +
        "LEFT(P.OTRF, 6)=" + (curVer >= 161 ? "' '" : "'0'") + "||F.FEFACT||F.FEFCNO AND " +
        "RIGHT(P.OTRF, 3)=TRIM(CHAR(F.FEFCOD))||RIGHT(P.OTRF, 1) AND P.CCY=F.FEFCCY AND '" + wrkDay + "' BETWEEN F.DAT AND F.DATTO AND " +
        "F.FEEID=(SELECT MAX(FEEID) FROM " + schemaDWH + ".FEETAB WHERE F.FELOAN=FELOAN AND F.FECNUM=FECNUM AND F.FEFCOD=FEFCOD AND " +
        "F.FEFCCY=FEFCCY AND '" + wrkDay + "' BETWEEN DAT AND DATTO) " +
      "JOIN " + schemaDWH + ".DELOTAB D ON F.FEEID=D.ID " +

      "where P.ID IN (" + pdloadids + ") AND P.OTST='' AND P.STATUS IN ('I','M') AND P.PDID=0 AND P.DLTYPE='C' " +
      " ";

    String sql4 = "update " + schemaDWH + "." + PDLOAD + " p set " +
      "fxbc=     COALESCE(FXBC,      (select d.fxbc      from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlvdat=   COALESCE(DLVDAT,    (select d.dlvdat    from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlmdat=   COALESCE(DLMDAT,    (select d.dlmdat    from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlid=     COALESCE(DLID,      (select d.dlid      from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlcnum=   COALESCE(DLCNUM,    (select d.dlcnum    from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlored=   COALESCE(DLORED,    (select d.dlored    from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlvalue=  COALESCE(DLVALUE,   (select d.dlvalue   from SESSION.DEALS d WHERE P.ID=D.ID)), " +
//      "RECI=     COALESCE(RECI,      (select d.RECI      from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "DLTYPE=   COALESCE(DLTYPE,    (select d.DL        from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "ftyp=     COALESCE(FTYP,      (select d.ftyp      from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "fseq=     COALESCE(FSEQ,      (select d.fseq      from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "dlno=     COALESCE(DLNO,      (select d.DLNO      from SESSION.DEALS d WHERE P.ID=D.ID)), " +
      "fcnum=    COALESCE(FCNUM,     (select d.fcnum     from SESSION.DEALS d WHERE P.ID=D.ID))  " +
      "where P.ID IN (" + pdloadids + ") ";

    String sql5 = "DELETE FROM SESSION.DEALS";
    return new String[]{sql1, sql2, sql3, sql4, sql5};
  }

  /**
   * �������� SQL ��� �������� ��������� ����� � PDLOAD �� ������ ������ � ��������� ���� (CE). <BR>
   * @param wrkDay ���� ����.���; <BR>
   * @return SQL ��� �������� ��������� �����. <BR>
   */
  public String getSQLForFillingDealFieldsCE(java.sql.Date wrkDay) {
    String sql =
      "update " + schemaDWH + "." + PDLOAD + " p set " +
      "FXSC=COALESCE(FXSC, select cu.drcy from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C')), " +
      "FXPC=COALESCE(FXPC, select cu.crcy from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C')), " +
      "FXSA=COALESCE(FXSA, Select cu.dram from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C')), " +
      "FXPA=COALESCE(FXPA, select cu.cram from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C')), " +
      "FXBC=COALESCE(FXBC, select cu.brca from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C')), " +
      "CNUMCE_DB=COALESCE(CNUMCE_DB, select DIGITS(cu.dcus) from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C')), " +
      "CNUMCE_CR=COALESCE(CNUMCE_CR, select DIGITS(cu.ccus) from " + schemaDWHIN + ".cusexce  cu  WHERE p.otrf=cu.ipdn AND CU.RECI IN ('D','C'))  " +
      "where p.pod='" + wrkDay.toString() + "' and P.OTRF=? AND P.PBR='GE-CE' ";
    return sql;
  }

////////////////////////////////////////////////////////////////////////////////
  public static void main(String[] str) {
    java.util.Locale.setDefault(new java.util.Locale("ru"));
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Europe/Moscow"));
    try {
      // initialize connection
      ConnectionFactory.getFactory().initConnection("connection.properties");
      org.apache.log4j.PropertyConfigurator.configure("logger.properties");

      // set working day
      SimpleDateFormat sf = new SimpleDateFormat("dd.MM.yyyy");
      java.sql.Date wrkDay = new java.sql.Date(sf.parse((str == null || str.length<1 || str[0] == null ? "10.05.2014" : str[0])).getTime());

//      lv.gcpartners.bank.util.CallCommandOnAS400 prgAS400 = new lv.gcpartners.bank.util.CallCommandOnAS400();
//      String cmd1 = "CLRPFM FILE(RUBARS01/EODPOPD)";
//      prgAS400.call(cmd1, null, null);
      LoadPostings process = new LoadPostings();
      process.initialize(null);
      // ���������� ����� ������ ����
      process.curVer = Integer.parseInt(String.valueOf(process.getParam("BARSCodeVersion", wrkDay, process.connection, process.schemaDWH)));
//      process.fillEODPOPD(wrkDay, "RUBARS01.TMPEODPOPD", "MS110503.EODPOPD");
//      process.gatherDeals(process.connection, wrkDay, "RUBARS01.TMPEODPOPD", "RUBARS01.TMPEODPD");
//      /** ����� �������������� doInsertPDLOAD ��������� �������� � RUBARS01.EODPOPD */
//      process.doInsertPDLOAD(wrkDay, "RUBARS01", "GTTBR", wrkDay, "left(trim(e.spos),3)='182'");
//      System.out.println(process.getSQL(wrkDay, "RUBARS01.TMP98765", "ZRUZSTG.EODPOPD", "ZRUZSTG.EODPODEALS", "ZRUZSTG.ACCNTAB", "E.SPOS='GE-DL'", "DL"));
//      System.out.println(process.getSQLOld("RUBARS01.TMPEODPOPD", "RUBARS01.PDLOAD", wrkDay, "BATCH", wrkDay));
//      process.deletePastDayBatch(wrkDay, process.schemaDWH + "." + process.PDLOAD);
//      process.startProcess(java.sql.Date.valueOf("2006-10-24"));
//      process.loadPostings(null, process.schemaDWH + "." + process.PDLOAD, wrkDay);
//      process.setRbdnDeal(wrkDay, process.schemaDWH + ".PDLOAD4", null);
      
      process.process("E1BARS01.TMPPDL0510", "E1BARS01.TMPEODPOPD", "E1DWH.eodpopd", null, wrkDay, "E1BARS01.TMPEODPD", "E1BARS01.ACCNTAB", null, true, true, true, true);
//      process.repPDLOAD_PNARR(wrkDay, "RUBARS01.TMPPDL1130");
//      System.out.println(process.getSQL(java.sql.Date.valueOf("2010-05-21"), "RUBARS01.PDLOAD", "RUBARS01.TMPEODPOPD",
//          "RUBARS01.TMPEODPD", "RUBARS01.ACCNTAB", null, Constants.BATCH, 0));
//      System.out.println(process.getSQL(wrkDay, "RUBARS01.TMPPDL84", "RUBARS01.EODPOPD", "RUBARS01.EODPODEALS", "RUBARS01.ACCNTAB", null, "BATCH", 0));
//      process.checkLoadedPstsCnt(wrkDay, "RUBARS01.TMPPDL5111", "RUDWH.EODPOPD");
//      process.cpyP2BLST(process.connection);

      process.closeResource();

    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit( -1);
    }
    System.exit(0);
  }
}