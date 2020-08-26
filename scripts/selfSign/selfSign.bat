:: Remove old Signature
7z d %NIAGARA_HOME%\modules\btibBigQuery-rt.jar META-INF\*.RSA META-INF\*.SF -bso0
7z d %NIAGARA_HOME%\modules\btibBigQuery-wb.jar META-INF\*.RSA META-INF\*.SF -bso0

:: Sign with Self generated on
jarsigner.exe -keystore scripts/selfSign/btibKeyStore -storepass  m9rCDFHe3gtx -keypass  m9rCDFHe3gtx -verbose %NIAGARA_HOME%\modules\btibBigQuery-rt.jar btib
jarsigner.exe -keystore scripts/selfSign/btibKeyStore -storepass  m9rCDFHe3gtx -keypass  m9rCDFHe3gtx -verbose %NIAGARA_HOME%\modules\btibBigQuery-wb.jar btib

:: Verify
jarsigner -verify %NIAGARA_HOME%\modules\btibBigQuery-rt.jar
jarsigner -verify %NIAGARA_HOME%\modules\btibBigQuery-wb.jar