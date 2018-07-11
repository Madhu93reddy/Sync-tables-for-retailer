# Sync-tables-for-retailer
Sync tables 

USE [ALineAtlantic]
GO

/****** Object:  StoredProcedure [dbo].[ep_ins_SyncTables]    Script Date: 7/11/2018 7:59:34 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/* +-----------------------------------------------------------------------------------------------------------------+ */
/* | Release:  Merlin v01.00                                                                                         | */
/* | Author:   Thomas Antola, Patricia Donovan                                                                       | */
/* | Written:  03/28/2001                                                                                            | */
/* | Modified: 05/05/2005                                                                                            | */
/* | Parms:    @CycleTS         - Server unique timestamp assigned each iteration.                                   | */
/* |           @Corporate       - Supplier's corporation.                                                            | */
/* |           @WarehouseMask   - Supplier's division warehouse mask.                                                | */
/* | Purpose:  This Procedure updates tables in the datawarehouse from the data in the dataAcquisition Database.     | */
/* +-----------------------------------------------------------------------------------------------------------------+ */
CREATE    PROCEDURE [dbo].[ep_ins_SyncTables] (@CycleTS CHAR(14), @Corporate VARCHAR(35) = NULL, @WarehouseMask INT = NULL, @STradingPartnerFK SMALLINT = NULL, @ReportDate SMALLDATETIME = NULL) AS
BEGIN

  SET NOCOUNT ON
  SET XACT_ABORT ON

  /* +--------------------------+ */
  /* | Create Temporary Tables. | */
  /* +--------------------------+ */
  CREATE TABLE dbo.#TRADING_PARTNER
  (
   TradingPartnerPK	SMALLINT	NOT NULL PRIMARY KEY
  )

  CREATE TABLE dbo.#INTERCHANGE
  (
   InterchangePK	INT		NOT NULL  PRIMARY KEY,
   PartnershipFK 	SMALLINT	NOT NULL,
   StateInd		CHAR     (1)	NOT NULL,
   QRLeadTime		SMALLINT		NOT NULL,
   AC			MONEY		NULL,
   DC			MONEY		NULL,
   IT			INT		NULL,
   QA			INT		NULL,
   QN			INT		NULL,
   QP			INT		NULL,
   QR			INT		NULL,
   QS			INT		NULL,
   QT			INT		NULL,
   QU			INT		NULL,
   RR			INT		NULL,
   XR			MONEY		NULL,
   [OR]			MONEY	NULL
  )

  CREATE TABLE dbo.#INTERCHANGE_HASH
  (
   InterchangePK	INT		NOT NULL  PRIMARY KEY,
   AC			MONEY		NULL,
   DC			MONEY		NULL,
   IT			INT		NULL,
   QA			INT		NULL,
   QN			INT		NULL,
   QP			INT		NULL,
   QR			INT		NULL,
   QS			INT		NULL,
   QT			INT		NULL,
   QU			INT		NULL,
   RR			INT		NULL,
   XR			MONEY		NULL,
   [OR]			MONEY	NULL
  )

  DECLARE @BuyingGroup		VARCHAR(35),
          @StartTS		DATETIME,
          @EndTS		DATETIME,
          @Database		SYSNAME,
          @PartnershipList	VARCHAR(512)

  SELECT @StartTS = CURRENT_TIMESTAMP

  /* +----------------------------------------------+ */
  /* | Load Trading Partner Keys For This Supplier. | */
  /* +----------------------------------------------+ */
  SELECT @BuyingGroup = DB_NAME()

  INSERT INTO dbo.#TRADING_PARTNER (TradingPartnerPK)
  EXEC [carda1].DataAcquisition.dbo.ep_sel_TradingPartner @BuyingGroup

  /* +-----------------------------+ */
  /* | Replicate Trading Partners. | */
  /* +-----------------------------+ */
  UPDATE TP
     SET BuyingGroup		= DA.BuyingGroup,
         TradingPartner		= DA.TradingPartner, 
         SupplierRetailerInd	= DA.SupplierRetailerInd,
         IncrementalReporting	= DA.IncrementalReporting,
         MaxSalesToleranceDP	= DA.MaxSalesToleranceDP,
         ToleranceRange		= DA.ToleranceRange,
         UpdateTS		= CURRENT_TIMESTAMP,
         UpdateUserID		= SYSTEM_USER
    FROM dbo.#TRADING_PARTNER STP INNER JOIN dbo.TRADING_PARTNER TP ON STP.TradingPartnerPK = TP.TradingPartnerPK
                                  INNER JOIN [carda1].DataAcquisition.dbo.TRADING_PARTNER DA ON TP.TradingPartnerPK = DA.TradingPartnerPK
   WHERE (DA.UpdateTS IS NOT NULL AND TP.UpdateTS IS NULL)	-- For first time updated.
      OR DA.UpdateTS > TP.UpdateTS				-- For all other times.

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Update of TRADING_PARTNER failed.', 16, 1)
    RETURN
  END

  INSERT INTO dbo.TRADING_PARTNER (TradingPartnerPK, BuyingGroup, TradingPartner, SupplierRetailerInd, IncrementalReporting, MaxSalesToleranceDP, ToleranceRange)
  SELECT DA.TradingPartnerPK, DA.BuyingGroup, DA.TradingPartner, DA.SupplierRetailerInd, DA.IncrementalReporting, DA.MaxSalesToleranceDP, DA.ToleranceRange
    FROM dbo.#TRADING_PARTNER STP INNER JOIN [carda1].DataAcquisition.dbo.TRADING_PARTNER DA ON STP.TradingPartnerPK = DA.TradingPartnerPK
                                   LEFT JOIN dbo.TRADING_PARTNER TP ON DA.TradingPartnerPK = TP.TradingPartnerPK
   WHERE TP.TradingPartnerPK IS NULL

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Insert into TRADING_PARTNER failed.', 16, 1)
    RETURN
  END

  /* +-------------------------+ */
  /* | Replicate Partnerships. | */
  /* +-------------------------+ */
  UPDATE P
     SET STradingPartnerFK		= DA.STradingPartnerFK,
         RTradingPartnerFK		= DA.RTradingPartnerFK, 
	 QSExtRetail			= DA.QSExtRetail,
         QUExtRetail			= DA.QUExtRetail,
         CustomerReturns		= DA.CustomerReturns,
         DuplicationWeeks		= DA.DuplicationWeeks,
         FoldingHistoricalWeeks		= DA.FoldingHistoricalWeeks,
         FoldHistoricalWeeks		= DA.FoldHistoricalWeeks,
         QALookbackWeeks		= DA.QALookbackWeeks,
         QSLookbackWeeks		= DA.QSLookbackWeeks,
         QAInd				= DA.QAInd,
         QRInd				= DA.QRInd,
         QRLeadTime			= DA.QRLeadTime,
         STransmitsDuplicates		= DA.STransmitsDuplicates,
         RTransmitsDuplicates		= DA.RTransmitsDuplicates,
         WarehouseFlags			= DA.WarehouseFlags,
         ExpectedFiles			= DA.ExpectedFiles,
         ExpectedFilesOverride		= DA.ExpectedFilesOverride,
         ExpectedFilesReceiptDeadline	= DA.ExpectedFilesReceiptDeadline,
         ExtractRequired		= DA.ExtractRequired,
         ReceivedThroughAcquisition     = DA.ReceivedThroughAcquisition,
         PilotCommenceDate		= DA.PilotCommenceDate,
         PilotApprovedDate		= DA.PilotApprovedDate,
         PilotWeek			= DA.PilotWeek,
         PSReportLeadTime               = DA.PSReportLeadTime,
         PSReportTimeWindow             = DA.PSReportTimeWindow,
         PSMinInclusionQty              = DA.PSMinInclusionQty,
         ProvidesShipments              = DA.ProvidesShipments,
         MeasureFileOverride		= DA.MeasureFileOverride,
         ToleranceSalesWindow           = DA.ToleranceSalesWindow,
         MissingTolerancePercent        = DA.MissingTolerancePercent,
         MonthlyBilling			= DA.MonthlyBilling,
         InsertTS			= DA.InsertTS,
         UpdateTS			= CURRENT_TIMESTAMP,
         UpdateUserID			= SYSTEM_USER,
		 CostValueInd					= DA.CostValueInd,
		 RetailValueInd					= DA.RetailValueInd,
		 RetailSalesInd					= DA.RetailSalesInd,
		 ReceiptUnitsInd				= DA.ReceiptUnitsInd

    FROM dbo.#TRADING_PARTNER STP INNER JOIN dbo.PARTNERSHIP P ON STP.TradingPartnerPK = P.STradingPartnerFK
                                  INNER JOIN [carda1].DataAcquisition.dbo.PARTNERSHIP DA ON P.PartnershipPK = DA.PartnershipPK
   WHERE (DA.UpdateTS IS NOT NULL AND P.UpdateTS IS NULL)	-- For first time updated.
      OR DA.UpdateTS > P.UpdateTS				-- For all other times.

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Update of PARTNERSHIP failed.', 16, 1)
    RETURN
  END

  INSERT INTO PARTNERSHIP (PartnershipPK, STradingPartnerFK, RTradingPartnerFK, QSExtRetail, QUExtRetail, CustomerReturns, DuplicationWeeks, FoldingHistoricalWeeks, FoldHistoricalWeeks, QALookbackWeeks, QAInd, QRInd, QRLeadTime, STransmitsDuplicates, RTransmitsDuplicates, WarehouseFlags, ExpectedFiles, ExpectedFilesOverride, ExpectedFilesReceiptDeadline, ExpectedFilesReportDeadline, ExtractRequired, PilotCommenceDate, PilotApprovedDate, PilotWeek, PSReportLeadTime, PSReportTimeWindow, PSMinInclusionQty, ProvidesShipments, MeasureFileOverride, ToleranceSalesWindow, MissingTolerancePercent, MonthlyBilling, CostValueInd, RetailValueInd, RetailSalesInd, ReceiptUnitsInd)
  SELECT DA.PartnershipPK, DA.STradingPartnerFK, DA.RTradingPartnerFK, DA.QSExtRetail, DA.QUExtRetail, DA.CustomerReturns, DA.DuplicationWeeks, DA.FoldingHistoricalWeeks, DA.FoldHistoricalWeeks, DA.QALookbackWeeks, DA.QAInd, DA.QRInd, DA.QRLeadTime, DA.STransmitsDuplicates, DA.RTransmitsDuplicates, DA.WarehouseFlags, DA.ExpectedFiles, DA.ExpectedFilesOverride, DA.ExpectedFilesReceiptDeadline, DA.ExpectedFilesReportDeadline, DA.ExtractRequired, DA.PilotCommenceDate, DA.PilotApprovedDate, DA.PilotWeek, DA.PSReportLeadTime, DA.PSReportTimeWindow, DA.PSMinInclusionQty, DA.ProvidesShipments, DA.MeasureFileOverride, DA.ToleranceSalesWindow, DA.MissingTolerancePercent, DA.MonthlyBilling, DA.CostValueInd, DA.RetailValueInd, DA.RetailSalesInd, DA.ReceiptUnitsInd
    FROM dbo.#TRADING_PARTNER STP INNER JOIN [carda1].DataAcquisition.dbo.PARTNERSHIP DA ON STP.TradingPartnerPK = DA.STradingPartnerFK
                                   LEFT JOIN dbo.PARTNERSHIP P ON DA.PartnershipPK = P.PartnershipPK
   WHERE P.PartnershipPK IS NULL

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Insert into PARTNERSHIP failed.', 16, 1)
    RETURN
  END

  /* +----------------------+ */
  /* | Replicate Locations. | */
  /* +----------------------+ */
  UPDATE L
     SET TradingPartnerFK	= DA.TradingPartnerFK,
         Account		= DA.Account,
         StoreNumber		= DA.StoreNumber,
         StoreName		= DA.StoreName,
         Address1		= DA.Address1,
         Address2		= DA.Address2,
         City			= DA.City,
         State			= DA.State,
         ZipCode		= DA.ZipCode,
         OpenDate		= DA.OpenDate,
         CloseDate		= DA.CloseDate,
         Mall			= DA.Mall,
         UpdateUserID		= DA.UpdateUserID,
         UpdateTS		= DA.UpdateTS
    FROM dbo.LOCATION L INNER JOIN dbo.#TRADING_PARTNER TP ON L.TradingPartnerFK = TP.TradingPartnerPK
                        INNER JOIN [carda1].DataAcquisition.dbo.LOCATION DA ON L.LocationPK = DA.LocationPK
   WHERE (DA.UpdateTS IS NOT NULL AND L.UpdateTS IS NULL)	-- For first time updated.
      OR DA.UpdateTS > L.UpdateTS				-- For all other times.

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Update of LOCATION failed.', 16, 1)
    RETURN
  END

  INSERT INTO LOCATION (LocationPK, TradingPartnerFK, Account, StoreNumber, StoreName, Address1, Address2, City, State, ZipCode, OpenDate, CloseDate, Mall)
  SELECT DA.LocationPK, DA.TradingPartnerFK, DA.Account, DA.StoreNumber, DA.StoreName, DA.Address1, DA.Address2, DA.City, DA.State, DA.ZipCode, DA.OpenDate, DA.CloseDate, DA.Mall
    FROM [carda1].DataAcquisition.dbo.LOCATION DA INNER JOIN dbo.#TRADING_PARTNER TP ON DA.TradingPartnerFK = TP.TradingPartnerPK
                                                   LEFT JOIN dbo.LOCATION L ON DA.LocationPK = L.LocationPK
   WHERE L.LocationPK IS NULL

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Insert into LOCATION failed.', 16, 1)
    RETURN
  END

  /* +-------------------------+ */
  /* | Replicate Interchanges. | */
  /* +-------------------------+ */
  INSERT dbo.#INTERCHANGE (InterchangePK, PartnershipFK, StateInd, QRLeadTime, AC, DC, IT, QA, QN, QP, QR, QS, QT, QU, RR, XR, [OR])
  EXEC [carda1].DataAcquisition.dbo.ep_sel_INTERCHANGE_OR @BuyingGroup, @Corporate, @WarehouseMask

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Insert into dbo.#INTERCHANGE failed.', 16, 1)
    RETURN
  END

  UPDATE I
     SET PartnershipFK	= DA.PartnershipFK,
         StateInd	= DA.StateInd
    FROM dbo.#INTERCHANGE DA INNER JOIN dbo.INTERCHANGE I ON DA.InterchangePK = I.InterchangePK

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Update of INTERCHANGE failed.', 16, 1)
    RETURN
  END

  INSERT INTO dbo.INTERCHANGE (InterchangePK, PartnershipFK, StateInd, ReleaseInd) 
  SELECT DA.InterchangePK, DA.PartnershipFK, DA.StateInd, CASE WHEN P.QAInd = 'P' AND DA.QA <> 0 THEN 'F' ELSE 'T' END
    FROM dbo.#INTERCHANGE DA INNER JOIN dbo.PARTNERSHIP P ON DA.PartnershipFK = P.PartnershipPK
                             LEFT JOIN dbo.INTERCHANGE I ON DA.InterchangePK = I.InterchangePK
   WHERE I.InterchangePK IS NULL

  IF @@ERROR <> 0
  BEGIN
    RAISERROR ('Insert into INTERCHANGE failed.', 16, 1)
    RETURN
  END

  BEGIN TRANSACTION

    /* +-------------------------------+ */
    /* | Replicate QA FOLDING Measure. | */
    /* +-------------------------------+ */
    INSERT INTO dbo.FOLDING (InterchangeFK, GroupCntlNo, TransSetCntlNo, DepartmentNo, Location, Man_Number, ShipDate, ArrivalDate, PONo, BOL, UPC, EAN, GTIN, ProductIDQual1, ProductID1, ProductIDQual2, ProductID2, ProductIDQual3, ProductID3, ProductIDQual4, ProductID4, UOM, ACCost, DCCost, ITQty, QAQty, QNQty, QOQty, QPQty, QRQty, QSQty, QSPrice, QTQty, QTPrice, QUQty, QUPrice, RRQty, ORPrice, Date, AdjustedDate)
    EXECUTE [carda1].DataAcquisition.dbo.ep_sel_FOLDING_OR  @BuyingGroup, @Corporate, @WarehouseMask
    IF @@ERROR <> 0
    BEGIN
      RAISERROR ('Insert into FOLDING failed.', 16, 1)
      ROLLBACK TRANSACTION
      RETURN
    END


    /* +--------------------------------+ */
    /* | Perform Quality Control Check. | */
    /* +--------------------------------+ */
    INSERT INTO dbo.#INTERCHANGE_HASH (InterchangePK, AC, DC, IT, QA, QN, QP, QR, QS, QT, QU, RR, XR, [OR])
    SELECT I.InterchangePK,
           ISNULL(SUM(CAST(Q.ACCost AS MONEY)),0) 'AC',
           ISNULL(SUM(CAST(Q.DCCost AS MONEY)),0) 'DC',
           ISNULL(SUM(CAST(Q.ITQty AS INT)),0) 'IT',           
           ISNULL(SUM(CAST(Q.QAQty AS INT)),0) 'QA',
           ISNULL(SUM(CAST(Q.QNQty AS INT)),0) 'QN',
           ISNULL(SUM(CAST(Q.QPQty AS INT)),0) 'QP', 
           ISNULL(SUM(CAST(Q.QRQty AS INT)),0) 'QR', 
           ISNULL(SUM(CAST(Q.QSQty AS INT)),0) 'QS',
           ISNULL(SUM(CAST(Q.QTQty AS INT)),0) 'QT',
           ISNULL(SUM(CAST(Q.QUQty AS INT)),0) 'QU',
           ISNULL(SUM(CAST(Q.RRQty AS INT)),0) 'RR',
           SUM(CAST(ISNULL(Q.QSPrice, 0) AS MONEY)) + SUM(CAST(ISNULL(Q.QTPrice, 0) AS MONEY)) + SUM(CAST(ISNULL(Q.QUPrice, 0) AS MONEY)) 'XR',
	       ISNULL(SUM(CAST(Q.ORPrice AS MONEY)),0) 'OR'
		
              FROM dbo.#INTERCHANGE I INNER JOIN dbo.FOLDING Q ON I.InterchangePK = Q.InterchangeFK
    GROUP BY I.InterchangePK

    IF @@ERROR <> 0
    BEGIN
      RAISERROR ('Insert into #INTERCHANGE_HASH failed.', 16, 1)
      ROLLBACK TRANSACTION
      RETURN
    END

    IF (SELECT COUNT(*)
          FROM dbo.#INTERCHANGE I INNER JOIN dbo.#INTERCHANGE_HASH IH ON I.InterchangePK = IH.InterchangePK
         WHERE I.AC <> IH.AC
            OR I.DC <> IH.DC
            OR I.IT <> IH.IT
            OR I.QA <> IH.QA
            OR I.QN <> IH.QN
            OR I.QP <> IH.QP
            OR I.QR <> IH.QR
            OR I.QS <> IH.QS
            OR I.QT <> IH.QT
            OR I.QU <> IH.QU
            OR I.RR <> IH.RR
            OR I.XR <> IH.XR
			OR I.[OR] <> IH.[OR]) <> 0
    BEGIN
      RAISERROR ('Quality control check failed.', 16, 1)
      ROLLBACK TRANSACTION
      RETURN
    END

    /* +------------------------------------------+ */
    /* | Flag Tables Indicating Retrieved Status. | */
    /* +------------------------------------------+ */
    UPDATE DA
       SET StateInd	  = CASE WHEN DA.StateInd = 'N' THEN 'W'
                                 WHEN DA.StateInd = 'C' THEN 'X'
                                 ELSE DA.StateInd
                            END,
           WarehouseFlags = DA.WarehouseFlags | ISNULL(@WarehouseMask, 0)
      FROM dbo.#INTERCHANGE I INNER JOIN [carda1].DataAcquisition.dbo.INTERCHANGE DA ON I.InterchangePK = DA.InterchangePK

    IF @@ERROR <> 0
    BEGIN
      RAISERROR ('Update of [carda1].DataAcquisition.dbo.INTERCHANGE failed.', 16, 1)
      ROLLBACK TRANSACTION
      RETURN
    END

    UPDATE IT
       SET WarehousedTS	= CURRENT_TIMESTAMP
      FROM dbo.#INTERCHANGE I INNER JOIN [carda1].DataAcquisition.dbo.INTERCHANGE_TRACKING IT ON I.InterchangePK = IT.InterchangeFK

    IF @@ERROR <> 0
    BEGIN
      RAISERROR ('Update of [carda1].DataAcquisition.dbo.INTERCHANGE_TRACKING failed.', 16, 1)
      ROLLBACK TRANSACTION
      RETURN
    END

    SELECT @EndTS	= CURRENT_TIMESTAMP,
           @Database	= DB_NAME()

	/*Fill Time Retailer table*/
	EXEC [carda1].[Ibis].dbo.ep_ins_SystemTracking @STradingPartnerFK, NULL, @ReportDate, @CycleTS, 'ep_ins_TimeRetailer', 'I'
		EXECUTE dbo.ep_ins_TimeRetailer
		  IF @@ERROR <> 0
		  BEGIN
			RAISERROR ('Call to dbo.ep_ins_TimeRetailer failed.', 16, 1)
			RETURN
		  END
	EXEC [carda1].[Ibis].dbo.ep_ins_SystemTracking @STradingPartnerFK, NULL, @ReportDate, @CycleTS, 'ep_ins_TimeRetailer', 'U'

	/*Fill Time Supplier table*/
	IF @STradingPartnerFK IS NOT NULL
	EXEC [carda1].[Ibis].dbo.ep_ins_SystemTracking @STradingPartnerFK, NULL, @ReportDate, @CycleTS, 'ep_ins_TimeSupplier', 'I'

		EXECUTE dbo.ep_ins_TimeSupplier
		  IF @@ERROR <> 0
		  BEGIN
			RAISERROR ('Call to dbo.ep_ins_TimeSupplier failed.', 16, 1)
			RETURN
		  END

	IF @STradingPartnerFK IS NOT NULL
	EXEC [carda1].[Ibis].dbo.ep_ins_SystemTracking @STradingPartnerFK, NULL, @ReportDate, @CycleTS, 'ep_ins_TimeSupplier', 'U'

    /* Builds a comma delimited list of partnership keys. */
    SELECT @PartnershipList = COALESCE(CAST(PartnershipFK AS VARCHAR(6)) + ',' + @PartnershipList, CAST(PartnershipFK AS VARCHAR(6)))
      FROM dbo.#INTERCHANGE
     GROUP BY PartnershipFK
     ORDER BY PartnershipFK DESC

    EXECUTE [carda1].[DataReporting].dbo.ep_ins_MessageCenter
            @ToDistributionList	= 'DataAcquisitionAdmins',
            @Subject		= 'ENS - Tracking.',
            @Message		= 'Tracking',
            @SubType		= 'T',
            @Server		= @@SERVERNAME,
            @Database		= @Database,
            @CycleTS		= @CycleTS,
            @Task		= 'ep_ins_SyncTables',
            @StartTS		= @StartTS,
            @EndTS		= @EndTS,
            @PartnershipList	= @PartnershipList

  COMMIT TRANSACTION

  /* +------------------------------+ */
  /* | Release Temporary Resources. | */
  /* +------------------------------+ */
  DROP TABLE dbo.#TRADING_PARTNER
  DROP TABLE dbo.#INTERCHANGE
  DROP TABLE dbo.#INTERCHANGE_HASH

END
GO



