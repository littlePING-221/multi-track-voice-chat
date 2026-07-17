use anyhow::{Context, Result};
use hound::{SampleFormat, WavSpec, WavWriter};
use std::{
    fs::{self, File},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

pub const SAMPLE_RATE: u32 = 48_000;

/// Downmix interleaved stereo S16LE frames to mono S16LE samples.
pub fn downmix_stereo_s16le(input: &[u8], output: &mut Vec<u8>) -> usize {
    let frames = input.len() / 4;
    output.reserve(frames * 2);
    for frame in input[..frames * 4].chunks_exact(4) {
        let left = i16::from_le_bytes([frame[0], frame[1]]) as i32;
        let right = i16::from_le_bytes([frame[2], frame[3]]) as i32;
        let mono = ((left + right) / 2) as i16;
        output.extend_from_slice(&mono.to_le_bytes());
    }
    frames
}

pub fn timeline_start_sample(first_ns: u64, recording_ns: u64) -> u64 {
    (((first_ns.saturating_sub(recording_ns) as f64) * SAMPLE_RATE as f64) / 1_000_000_000.0)
        .round() as u64
}

pub fn write_pcm(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).context("create PCM segment")?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub fn finalize_wav(
    tmp: &Path,
    output: &Path,
    segments: &[(u64, Vec<u8>)],
    total_samples: u64,
) -> Result<()> {
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent)?;
    }
    let spec = WavSpec {
        channels: 1,
        sample_rate: SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format: SampleFormat::Int,
    };
    let mut writer = WavWriter::create(tmp, spec)?;
    let mut cursor = 0u64;
    for (start, data) in segments {
        let start = (*start).min(total_samples);
        while cursor < start {
            writer.write_sample(0i16)?;
            cursor += 1;
        }
        for chunk in data.chunks_exact(2) {
            if cursor >= total_samples {
                break;
            }
            writer.write_sample(i16::from_le_bytes([chunk[0], chunk[1]]))?;
            cursor += 1;
        }
    }
    while cursor < total_samples {
        writer.write_sample(0i16)?;
        cursor += 1;
    }
    writer.finalize()?;
    fs::rename(tmp, output).context("atomically publish WAV")?;
    Ok(())
}

/// Streams PCM segment files into a single WAV without buffering a recording in memory.
pub fn finalize_wav_from_files(
    tmp: &Path,
    output: &Path,
    segments: &[(u64, PathBuf)],
    total_samples: u64,
) -> Result<()> {
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent)?;
    }
    let spec = WavSpec {
        channels: 1,
        sample_rate: SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format: SampleFormat::Int,
    };
    let mut writer = WavWriter::create(tmp, spec)?;
    let mut cursor = 0u64;
    for (start, path) in segments {
        let start = (*start).min(total_samples);
        while cursor < start {
            writer.write_sample(0i16)?;
            cursor += 1;
        }
        let mut reader = BufReader::new(
            File::open(path).with_context(|| format!("open PCM segment {}", path.display()))?,
        );
        let mut buffer = [0u8; 16 * 1024];
        let mut skipped = cursor.saturating_sub(start);
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 || cursor >= total_samples {
                break;
            }
            for chunk in buffer[..read].chunks_exact(2) {
                if skipped > 0 {
                    skipped -= 1;
                    continue;
                }
                if cursor >= total_samples {
                    break;
                }
                writer.write_sample(i16::from_le_bytes([chunk[0], chunk[1]]))?;
                cursor += 1;
            }
        }
    }
    while cursor < total_samples {
        writer.write_sample(0i16)?;
        cursor += 1;
    }
    writer.finalize()?;
    fs::rename(tmp, output).context("atomically publish WAV")?;
    Ok(())
}

pub fn append_event(path: &Path, event: &serde_json::Value) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, event)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

pub fn check_disk(path: &Path, minimum_bytes: u64) -> Result<()> {
    fs::create_dir_all(path)?;
    let available = fs2::available_space(path).context("check free disk space")?;
    if available < minimum_bytes {
        anyhow::bail!("insufficient recording disk space")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timeline_rounds_to_samples() {
        assert_eq!(timeline_start_sample(1_000_000_000, 0), 48_000);
    }

    #[test]
    fn stereo_s16le_is_downmixed_to_mono_frames() {
        let mut output = Vec::new();
        let frames = downmix_stereo_s16le(
            &[
                0xe8, 0x03, 0xd0, 0x07, // 1000, 2000
                0x18, 0xfc, 0x30, 0xf8, // -1000, -2000
                0xff, 0x7f, 0xff, 0x7f, // i16::MAX, i16::MAX
                0xaa, // incomplete frame must be retained by the caller
            ],
            &mut output,
        );
        let samples: Vec<i16> = output
            .chunks_exact(2)
            .map(|sample| i16::from_le_bytes([sample[0], sample[1]]))
            .collect();
        assert_eq!(frames, 3);
        assert_eq!(samples, vec![1500, -1500, i16::MAX]);
    }
    #[test]
    fn wav_tracks_are_equal_length() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.tmp");
        let ao = dir.path().join("a.wav");
        finalize_wav(&a, &ao, &[(2, vec![1, 0, 2, 0])], 4).unwrap();
        let reader = hound::WavReader::open(ao).unwrap();
        assert_eq!(reader.len(), 4);
    }

    #[test]
    fn streamed_wav_inserts_silence_before_a_late_segment() {
        let dir = tempfile::tempdir().unwrap();
        let pcm = dir.path().join("late.pcm");
        write_pcm(&pcm, &[1, 0, 2, 0]).unwrap();
        let tmp = dir.path().join("out.tmp");
        let output = dir.path().join("out.wav");
        finalize_wav_from_files(&tmp, &output, &[(2, pcm)], 5).unwrap();
        let samples: Vec<i16> = hound::WavReader::open(output)
            .unwrap()
            .into_samples::<i16>()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(samples, vec![0, 0, 1, 2, 0]);
    }
}
