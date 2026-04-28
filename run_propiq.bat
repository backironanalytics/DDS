@echo off
REM ============================================================
REM PropIQ Nightly Run — Task Scheduler Batch File
REM Save this as: C:\Users\ccrai\OneDrive\Documents\DDS\run_propiq.bat
REM ============================================================

REM Log start time
echo PropIQ starting at %DATE% %TIME% >> "C:\Users\ccrai\OneDrive\Documents\DDS\propiq_run.log"

REM Run the R script
"C:\Program Files\R\R-4.5.3\bin\Rscript.exe" "C:\Users\ccrai\OneDrive\Documents\DDS\PropIQ_nightly_run.R" >> "C:\Users\ccrai\OneDrive\Documents\DDS\propiq_run.log" 2>&1

REM Log completion
echo PropIQ finished at %DATE% %TIME% >> "C:\Users\ccrai\OneDrive\Documents\DDS\propiq_run.log"

REM Put computer to sleep after run completes
REM Comment this line out if you don't want auto-sleep
REM rundll32.exe powrprof.dll,SetSuspendState 0,1,0

exit
