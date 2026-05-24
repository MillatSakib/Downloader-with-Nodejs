# File Downloader

This is a file downloader using nodejs. Which can download any file.

Just drop the download fils links and name into link.txt, then run the server. It will automatically download the all files. The patter are given below:

```txt
https://example.com example1
https://example.com example2
https://example.com example3
```

You can also open the dashboard at `http://localhost:3000`, add a URL from the form, and download finished files from the completed list. Direct file downloads are split into 8 chunks when the server supports byte ranges, with each chunk shown inside one segmented progress bar. Active downloads can be paused, resumed, or canceled, and dashboard updates are pushed in real time with Socket.IO.
