package src.main.scala.utils.download

import _root_.java.io.File
import _root_.java.net.URL

import sys.process._

object DownloadUrlToFile {

  /*
   * function: downloadUrlToFile
   *
   * Downloads an URL to a local file (the contents of the URL are not
   * interpreted, ie., they are taken as-is, as raw binary
   */

  def downloadUrlToFile(src_url: String, dest_file: File) {
    // From:
    // http://alvinalexander.com/scala/scala-how-to-download-url-contents-to-string-file
    //
    new URL(src_url) #> dest_file !!
  }


  def apply(src_url: String, dest_file: File) =
    downloadUrlToFile(src_url, dest_file)

}

