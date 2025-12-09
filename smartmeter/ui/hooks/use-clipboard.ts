"use client"

import { useState, useCallback } from "react"

/**
 * Custom hook for copying text to clipboard with fallback support
 * Works in both HTTPS and HTTP contexts
 */
export function useClipboard() {
  const [copied, setCopied] = useState(false)

  const copyToClipboard = useCallback(async (text: string) => {
    try {
      // Try modern clipboard API first (requires HTTPS or localhost)
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
        return true
      }

      // Fallback for HTTP or older browsers
      const textArea = document.createElement("textarea")
      textArea.value = text
      textArea.style.position = "fixed"
      textArea.style.left = "-999999px"
      textArea.style.top = "-999999px"
      document.body.appendChild(textArea)
      textArea.focus()
      textArea.select()

      try {
        const successful = document.execCommand("copy")
        if (successful) {
          setCopied(true)
          setTimeout(() => setCopied(false), 2000)
          return true
        }
        return false
      } catch (err) {
        console.error("Error copying to clipboard:", err)
        return false
      } finally {
        document.body.removeChild(textArea)
      }
    } catch (error) {
      console.error("Error copying to clipboard:", error)
      return false
    }
  }, [])

  return { copied, copyToClipboard }
}

