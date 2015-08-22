package src.main.scala.utils.download

import sys.process._

import _root_.java.io.File
import _root_.java.net.URL


object Download_URL_to_File {

    /*
     * function: download_url_to_file
     *
     * Downloads an URL to a local file (the contents of the URL are not
     * interpreted, ie., they are taken as-is, as raw binary
     */

    def download_url_to_file(src_url: String, dest_file: File) {
          // From:
          // http://alvinalexander.com/scala/scala-how-to-download-url-contents-to-string-file
          //
          new URL(src_url) #> dest_file !!
    }


    def apply(src_url: String, dest_file: File) =
                                 download_url_to_file(src_url, dest_file)

}

