from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sqlite3
import hashlib
import jwt
import datetime
import os
import pandas as pd
import io
from functools import wraps

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = 'palet-takip-gizli-anahtar-2026'
CORS(app)

DB_NAME = "palet_takip.db"

# Palet tipleri (başlangıç)
PALET_TIPLERI = [
    ("P001", "Euro Palet"),
    ("P002", "Sanayi Paleti"),
    ("P003", "Plastik Palet")
]

# Stok sahibi tipleri
SAHIP_TIP_DEPO = "DEPO"
SAHIP_TIP_DAGITICI = "DAGITICI"
SAHIP_TIP_MUSTERI = "MUSTERI"

# Hareket tipleri
HAREKET_DEPO_DAGITICI = "DEPO_DAGITICI"
HAREKET_DAGITICI_MUSTERI = "DAGITICI_MUSTERI"
HAREKET_MUSTERI_DAGITICI = "MUSTERI_DAGITICI"
HAREKET_DAGITICI_DEPO = "DAGITICI_DEPO"


def hash_sifre(sifre):
    return hashlib.sha256(sifre.encode()).hexdigest()


def veritabani_olustur():
    """Veritabanı tablolarını oluştur"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Kullanıcılar tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kullanicilar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kullanici_adi TEXT UNIQUE NOT NULL,
            sifre TEXT NOT NULL,
            tip TEXT NOT NULL,
            ad_soyad TEXT NOT NULL
        )
    ''')

    # Müşteriler tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS musteriler (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            musteri_kodu TEXT UNIQUE NOT NULL,
            musteri_adi TEXT NOT NULL,
            bagli_dagitici_id INTEGER NOT NULL,
            FOREIGN KEY (bagli_dagitici_id) REFERENCES kullanicilar(id)
        )
    ''')

    # Palet tipleri tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS palet_tipleri (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stok_kodu TEXT UNIQUE NOT NULL,
            palet_adi TEXT NOT NULL
        )
    ''')

    # Stoklar tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stoklar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stok_sahibi_tip TEXT NOT NULL,
            stok_sahibi_id INTEGER NOT NULL,
            palet_tipi_id INTEGER NOT NULL,
            miktar INTEGER DEFAULT 0,
            FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id),
            UNIQUE(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id)
        )
    ''')

    # Hareketler tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hareketler (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tarih TEXT NOT NULL,
            yapan_kullanici_id INTEGER NOT NULL,
            hareket_tipi TEXT NOT NULL,
            gonderen_tip TEXT NOT NULL,
            gonderen_id INTEGER NOT NULL,
            alan_tip TEXT NOT NULL,
            alan_id INTEGER NOT NULL,
            palet_tipi_id INTEGER NOT NULL,
            miktar INTEGER NOT NULL,
            aciklama TEXT,
            FOREIGN KEY (yapan_kullanici_id) REFERENCES kullanicilar(id),
            FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id)
        )
    ''')

    conn.commit()

    # Palet tiplerini ekle
    for stok_kodu, palet_adi in PALET_TIPLERI:
        cursor.execute('''
            INSERT OR IGNORE INTO palet_tipleri (stok_kodu, palet_adi)
            VALUES (?, ?)
        ''', (stok_kodu, palet_adi))

    # Varsayılan depocu
    cursor.execute('''
        INSERT OR IGNORE INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad)
        VALUES (?, ?, ?, ?)
    ''', ('depocu', hash_sifre('1234'), 'DEPOCU', 'Ana Depocu'))

    conn.commit()

    # Depo stoklarını oluştur
    cursor.execute('SELECT id FROM palet_tipleri')
    paletler = cursor.fetchall()
    for palet in paletler:
        cursor.execute('''
            INSERT OR IGNORE INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
            VALUES (?, ?, ?, ?)
        ''', (SAHIP_TIP_DEPO, 0, palet[0], 0))

    conn.commit()
    conn.close()


# Token doğrulama decorator'ı
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'hata': 'Token gerekli'}), 401
        
        try:
            token = token.replace('Bearer ', '')
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data
        except:
            return jsonify({'hata': 'Geçersiz token'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated


def stok_miktari_getir(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT miktar FROM stoklar
        WHERE stok_sahibi_tip = ? AND stok_sahibi_id = ? AND palet_tipi_id = ?
    ''', (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    sonuc = cursor.fetchone()
    conn.close()
    return sonuc[0] if sonuc else 0


def stok_guncelle(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, degisim):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    mevcut = stok_miktari_getir(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id)
    yeni_miktar = mevcut + degisim
    if yeni_miktar < 0:
        conn.close()
        return False, "Stok yetersiz!"
    cursor.execute('''
        UPDATE stoklar SET miktar = ?
        WHERE stok_sahibi_tip = ? AND stok_sahibi_id = ? AND palet_tipi_id = ?
    ''', (yeni_miktar, stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    conn.commit()
    conn.close()
    return True, ""


def hareket_kaydet(yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id,
                    alan_tip, alan_id, palet_tipi_id, miktar, aciklama=""):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO hareketler (tarih, yapan_kullanici_id, hareket_tipi,
                                gonderen_tip, gonderen_id, alan_tip, alan_id,
                                palet_tipi_id, miktar, aciklama)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id,
          alan_tip, alan_id, palet_tipi_id, miktar, aciklama))
    conn.commit()
    conn.close()


# ==================== ANA SAYFA ====================

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


# ==================== KULLANICI İŞLEMLERİ ====================

@app.route('/api/login', methods=['POST'])
def login():
    """Kullanıcı girişi"""
    data = request.get_json()
    kullanici_adi = data.get('kullanici_adi')
    sifre = data.get('sifre')
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, kullanici_adi, tip, ad_soyad FROM kullanicilar
        WHERE kullanici_adi = ? AND sifre = ?
    ''', (kullanici_adi, hash_sifre(sifre)))
    kullanici = cursor.fetchone()
    conn.close()
    
    if kullanici:
        token = jwt.encode({
            'id': kullanici[0],
            'kullanici_adi': kullanici[1],
            'tip': kullanici[2],
            'ad_soyad': kullanici[3],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm='HS256')
        
        return jsonify({
            'success': True,
            'token': token,
            'kullanici': {
                'id': kullanici[0],
                'kullanici_adi': kullanici[1],
                'tip': kullanici[2],
                'ad_soyad': kullanici[3]
            }
        })
    else:
        return jsonify({'success': False, 'hata': 'Hatalı kullanıcı adı veya şifre'}), 401


@app.route('/api/kullanici_listesi', methods=['GET'])
@token_required
def get_kullanici_listesi(current_user):
    """Tüm dağıtıcıları listele (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, kullanici_adi, ad_soyad FROM kullanicilar
        WHERE tip = 'DAGITICI'
        ORDER BY ad_soyad
    ''')
    sonuc = cursor.fetchall()
    conn.close()
    
    kullanicilar = [{'id': k[0], 'kullanici_adi': k[1], 'ad_soyad': k[2]} for k in sonuc]
    return jsonify(kullanicilar)


@app.route('/api/kullanici_duzenle', methods=['PUT'])
@token_required
def kullanici_duzenle(current_user):
    """Dağıtıcı bilgilerini düzenle (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    kullanici_id = data.get('id')
    kullanici_adi = data.get('kullanici_adi')
    ad_soyad = data.get('ad_soyad')
    sifre = data.get('sifre')
    
    if not kullanici_id or not kullanici_adi or not ad_soyad:
        return jsonify({'hata': 'ID, kullanıcı adı ve ad soyad gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        if sifre and len(sifre) >= 4:
            cursor.execute('''
                UPDATE kullanicilar 
                SET kullanici_adi = ?, ad_soyad = ?, sifre = ?
                WHERE id = ? AND tip = 'DAGITICI'
            ''', (kullanici_adi, ad_soyad, hash_sifre(sifre), kullanici_id))
        else:
            cursor.execute('''
                UPDATE kullanicilar 
                SET kullanici_adi = ?, ad_soyad = ?
                WHERE id = ? AND tip = 'DAGITICI'
            ''', (kullanici_adi, ad_soyad, kullanici_id))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'hata': 'Kullanıcı bulunamadı'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'mesaj': 'Kullanıcı güncellendi'})
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'hata': 'Bu kullanıcı adı zaten kullanılıyor'}), 400


@app.route('/api/kullanici_sil', methods=['DELETE'])
@token_required
def kullanici_sil(current_user):
    """Dağıtıcı sil (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    kullanici_id = request.args.get('id', type=int)
    
    if not kullanici_id:
        return jsonify({'hata': 'ID gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Bu dağıtıcıya ait müşteri var mı?
    cursor.execute('SELECT COUNT(*) FROM musteriler WHERE bagli_dagitici_id = ?', (kullanici_id,))
    musteri_sayisi = cursor.fetchone()[0]
    
    if musteri_sayisi > 0:
        conn.close()
        return jsonify({'hata': f'Bu dağıtıcıya ait {musteri_sayisi} müşteri var. Önce müşterileri silin veya başka dağıtıcıya taşıyın!'}), 400
    
    # Dağıtıcının stoklarını sil
    cursor.execute('DELETE FROM stoklar WHERE stok_sahibi_tip = ? AND stok_sahibi_id = ?', 
                   (SAHIP_TIP_DAGITICI, kullanici_id))
    
    # Kullanıcıyı sil
    cursor.execute('DELETE FROM kullanicilar WHERE id = ? AND tip = ?', (kullanici_id, 'DAGITICI'))
    
    if cursor.rowcount == 0:
        conn.close()
        return jsonify({'hata': 'Kullanıcı bulunamadı'}), 404
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'mesaj': 'Kullanıcı silindi'})


@app.route('/api/dagitici_ekle', methods=['POST'])
@token_required
def dagitici_ekle(current_user):
    """Yeni dağıtıcı ekle (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    kullanici_adi = data.get('kullanici_adi')
    ad_soyad = data.get('ad_soyad')
    sifre = data.get('sifre')
    
    if not kullanici_adi or not ad_soyad or not sifre:
        return jsonify({'hata': 'Tüm alanlar gerekli'}), 400
    
    if len(sifre) < 4:
        return jsonify({'hata': 'Şifre en az 4 karakter olmalı'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad)
            VALUES (?, ?, ?, ?)
        ''', (kullanici_adi, hash_sifre(sifre), 'DAGITICI', ad_soyad))
        
        dagitici_id = cursor.lastrowid
        
        cursor.execute("SELECT id FROM palet_tipleri")
        paletler = cursor.fetchall()
        for palet in paletler:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (?, ?, ?, 0)
            ''', (SAHIP_TIP_DAGITICI, dagitici_id, palet[0]))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'id': dagitici_id, 'mesaj': 'Dağıtıcı eklendi'})
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'hata': 'Bu kullanıcı adı zaten kullanılıyor'}), 400


# ==================== MÜŞTERİ İŞLEMLERİ ====================

@app.route('/api/musteri_ekle', methods=['POST'])
@token_required
def musteri_ekle(current_user):
    """Yeni müşteri ekle (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    musteri_kodu = data.get('musteri_kodu')
    musteri_adi = data.get('musteri_adi')
    bagli_dagitici_id = data.get('bagli_dagitici_id')
    
    if not musteri_kodu or not musteri_adi or not bagli_dagitici_id:
        return jsonify({'hata': 'Tüm alanlar gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM kullanicilar WHERE id = ? AND tip = 'DAGITICI'", (bagli_dagitici_id,))
    if not cursor.fetchone():
        conn.close()
        return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
    
    try:
        cursor.execute('''
            INSERT INTO musteriler (musteri_kodu, musteri_adi, bagli_dagitici_id)
            VALUES (?, ?, ?)
        ''', (musteri_kodu, musteri_adi, bagli_dagitici_id))
        
        musteri_id = cursor.lastrowid
        
        cursor.execute("SELECT id FROM palet_tipleri")
        paletler = cursor.fetchall()
        for palet in paletler:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (?, ?, ?, 0)
            ''', (SAHIP_TIP_MUSTERI, musteri_id, palet[0]))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'id': musteri_id, 'mesaj': 'Müşteri eklendi'})
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'hata': 'Bu müşteri kodu zaten kullanılıyor'}), 400


@app.route('/api/musteri_listesi', methods=['GET'])
@token_required
def get_musteri_listesi(current_user):
    """Tüm müşterileri listele (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT m.id, m.musteri_kodu, m.musteri_adi, k.ad_soyad as bagli_dagitici
        FROM musteriler m
        JOIN kullanicilar k ON m.bagli_dagitici_id = k.id
        ORDER BY m.musteri_adi
    ''')
    sonuc = cursor.fetchall()
    conn.close()
    
    musteriler = [{'id': m[0], 'musteri_kodu': m[1], 'musteri_adi': m[2], 'bagli_dagitici': m[3]} for m in sonuc]
    return jsonify(musteriler)


@app.route('/api/dagitici_musterileri', methods=['GET'])
@token_required
def get_dagitici_musterileri(current_user):
    """Dağıtıcıya bağlı müşterileri listele"""
    if current_user['tip'] != 'DAGITICI':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, musteri_kodu, musteri_adi FROM musteriler
        WHERE bagli_dagitici_id = ?
    ''', (current_user['id'],))
    sonuc = cursor.fetchall()
    conn.close()
    
    musteriler = [{'id': m[0], 'musteri_kodu': m[1], 'musteri_adi': m[2]} for m in sonuc]
    return jsonify(musteriler)


@app.route('/api/musteri_sil', methods=['DELETE'])
@token_required
def musteri_sil(current_user):
    """Müşteri sil (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    musteri_id = request.args.get('id', type=int)
    
    if not musteri_id:
        return jsonify({'hata': 'ID gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Müşterinin stoklarını kontrol et
    cursor.execute('SELECT SUM(miktar) FROM stoklar WHERE stok_sahibi_tip = ? AND stok_sahibi_id = ?', 
                   (SAHIP_TIP_MUSTERI, musteri_id))
    toplam_stok = cursor.fetchone()[0]
    
    if toplam_stok and toplam_stok > 0:
        conn.close()
        return jsonify({'hata': f'Bu müşterinin {toplam_stok} adet paleti var. Önce stokları boşaltın!'}), 400
    
    # Müşterinin stoklarını sil
    cursor.execute('DELETE FROM stoklar WHERE stok_sahibi_tip = ? AND stok_sahibi_id = ?', 
                   (SAHIP_TIP_MUSTERI, musteri_id))
    
    # Müşteriyi sil
    cursor.execute('DELETE FROM musteriler WHERE id = ?', (musteri_id,))
    
    if cursor.rowcount == 0:
        conn.close()
        return jsonify({'hata': 'Müşteri bulunamadı'}), 404
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'mesaj': 'Müşteri silindi'})


# ==================== PALET TİPLERİ ====================

@app.route('/api/palet_tipleri', methods=['GET'])
@token_required
def get_palet_tipleri(current_user):
    """Palet tiplerini listele"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT id, stok_kodu, palet_adi FROM palet_tipleri")
    sonuc = cursor.fetchall()
    conn.close()
    
    paletler = [{'id': p[0], 'stok_kodu': p[1], 'palet_adi': p[2]} for p in sonuc]
    return jsonify(paletler)


@app.route('/api/palet_tipi_ekle', methods=['POST'])
@token_required
def palet_tipi_ekle(current_user):
    """Yeni palet tipi ekle (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    stok_kodu = data.get('stok_kodu')
    palet_adi = data.get('palet_adi')
    
    if not stok_kodu or not palet_adi:
        return jsonify({'hata': 'Stok kodu ve palet adı gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO palet_tipleri (stok_kodu, palet_adi)
            VALUES (?, ?)
        ''', (stok_kodu, palet_adi))
        
        palet_tipi_id = cursor.lastrowid
        
        # Tüm stok sahipleri için bu palet tipini ekle
        cursor.execute('''
            INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
            VALUES (?, ?, ?, 0)
        ''', (SAHIP_TIP_DEPO, 0, palet_tipi_id))
        
        cursor.execute('SELECT id FROM kullanicilar WHERE tip = "DAGITICI"')
        dagiticilar = cursor.fetchall()
        for d in dagiticilar:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (?, ?, ?, 0)
            ''', (SAHIP_TIP_DAGITICI, d[0], palet_tipi_id))
        
        cursor.execute('SELECT id FROM musteriler')
        musteriler = cursor.fetchall()
        for m in musteriler:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (?, ?, ?, 0)
            ''', (SAHIP_TIP_MUSTERI, m[0], palet_tipi_id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'id': palet_tipi_id, 'mesaj': 'Palet tipi eklendi'})
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'hata': 'Bu stok kodu zaten kullanılıyor'}), 400


@app.route('/api/palet_tipi_duzenle', methods=['PUT'])
@token_required
def palet_tipi_duzenle(current_user):
    """Palet tipi düzenle (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    palet_tipi_id = data.get('id')
    stok_kodu = data.get('stok_kodu')
    palet_adi = data.get('palet_adi')
    
    if not palet_tipi_id or not stok_kodu or not palet_adi:
        return jsonify({'hata': 'ID, stok kodu ve palet adı gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE palet_tipleri 
            SET stok_kodu = ?, palet_adi = ?
            WHERE id = ?
        ''', (stok_kodu, palet_adi, palet_tipi_id))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'hata': 'Palet tipi bulunamadı'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'mesaj': 'Palet tipi güncellendi'})
        
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'hata': 'Bu stok kodu zaten kullanılıyor'}), 400


@app.route('/api/palet_tipi_sil', methods=['DELETE'])
@token_required
def palet_tipi_sil(current_user):
    """Palet tipi sil (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    palet_tipi_id = request.args.get('id', type=int)
    
    if not palet_tipi_id:
        return jsonify({'hata': 'ID gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT SUM(miktar) FROM stoklar 
        WHERE palet_tipi_id = ? AND miktar > 0
    ''', (palet_tipi_id,))
    toplam = cursor.fetchone()[0]
    
    if toplam and toplam > 0:
        conn.close()
        return jsonify({'hata': f'Bu palet tipine ait {toplam} adet stok var. Önce stokları boşaltın!'}), 400
    
    try:
        cursor.execute('DELETE FROM stoklar WHERE palet_tipi_id = ?', (palet_tipi_id,))
        cursor.execute('DELETE FROM palet_tipleri WHERE id = ?', (palet_tipi_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'hata': 'Palet tipi bulunamadı'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'mesaj': 'Palet tipi silindi'})
        
    except Exception as e:
        conn.close()
        return jsonify({'hata': str(e)}), 400


# ==================== STOK İŞLEMLERİ ====================

@app.route('/api/stok', methods=['GET'])
@token_required
def get_stok(current_user):
    """Stok sorgula"""
    tip = request.args.get('tip')
    kimlik = request.args.get('id', type=int)
    
    if not tip or kimlik is None:
        return jsonify({'hata': 'tip ve id parametreleri gerekli'}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT pt.id, pt.stok_kodu, pt.palet_adi, s.miktar
        FROM palet_tipleri pt
        LEFT JOIN stoklar s ON pt.id = s.palet_tipi_id 
            AND s.stok_sahibi_tip = ? AND s.stok_sahibi_id = ?
        ORDER BY pt.id
    ''', (tip, kimlik))
    sonuc = cursor.fetchall()
    conn.close()
    
    stoklar = []
    for p in sonuc:
        stoklar.append({
            'palet_id': p[0],
            'stok_kodu': p[1],
            'palet_adi': p[2],
            'miktar': p[3] if p[3] else 0
        })
    
    return jsonify(stoklar)


@app.route('/api/transfer', methods=['POST'])
@token_required
def transfer_yap(current_user):
    """Transfer işlemi"""
    data = request.get_json()
    hareket_tipi = data.get('hareket_tipi')
    palet_tipi_id = data.get('palet_tipi_id')
    miktar = data.get('miktar')
    alici_id = data.get('alici_id')
    
    if not hareket_tipi or not palet_tipi_id or not miktar:
        return jsonify({'hata': 'Eksik parametreler'}), 400
    
    if miktar <= 0:
        return jsonify({'hata': 'Miktar pozitif olmalı'}), 400
    
    kullanici_id = current_user['id']
    kullanici_tip = current_user['tip']
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT stok_kodu, palet_adi FROM palet_tipleri WHERE id = ?", (palet_tipi_id,))
    palet = cursor.fetchone()
    if not palet:
        conn.close()
        return jsonify({'hata': 'Geçersiz palet tipi'}), 400
    
    if kullanici_tip == 'DEPOCU':
        if hareket_tipi == 'DEPO_DAGITICI':
            gonderen_tip = SAHIP_TIP_DEPO
            gonderen_id = 0
            alan_tip = SAHIP_TIP_DAGITICI
            alan_id = alici_id
            
            cursor.execute("SELECT id FROM kullanicilar WHERE id = ? AND tip = 'DAGITICI'", (alici_id,))
            if not cursor.fetchone():
                conn.close()
                return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
                
        elif hareket_tipi == 'DAGITICI_DEPO':
            gonderen_tip = SAHIP_TIP_DAGITICI
            gonderen_id = alici_id
            alan_tip = SAHIP_TIP_DEPO
            alan_id = 0
            
            cursor.execute("SELECT id FROM kullanicilar WHERE id = ? AND tip = 'DAGITICI'", (alici_id,))
            if not cursor.fetchone():
                conn.close()
                return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
        else:
            conn.close()
            return jsonify({'hata': 'Geçersiz hareket tipi'}), 400
            
    elif kullanici_tip == 'DAGITICI':
        if hareket_tipi == 'DAGITICI_MUSTERI':
            gonderen_tip = SAHIP_TIP_DAGITICI
            gonderen_id = kullanici_id
            alan_tip = SAHIP_TIP_MUSTERI
            alan_id = alici_id
            
            cursor.execute('''
                SELECT id FROM musteriler 
                WHERE id = ? AND bagli_dagitici_id = ?
            ''', (alici_id, kullanici_id))
            if not cursor.fetchone():
                conn.close()
                return jsonify({'hata': 'Geçersiz müşteri ID'}), 400
                
        elif hareket_tipi == 'MUSTERI_DAGITICI':
            gonderen_tip = SAHIP_TIP_MUSTERI
            gonderen_id = alici_id
            alan_tip = SAHIP_TIP_DAGITICI
            alan_id = kullanici_id
            
            cursor.execute('''
                SELECT id FROM musteriler 
                WHERE id = ? AND bagli_dagitici_id = ?
            ''', (alici_id, kullanici_id))
            if not cursor.fetchone():
                conn.close()
                return jsonify({'hata': 'Geçersiz müşteri ID'}), 400
                
        elif hareket_tipi == 'DAGITICI_DEPO':
            gonderen_tip = SAHIP_TIP_DAGITICI
            gonderen_id = kullanici_id
            alan_tip = SAHIP_TIP_DEPO
            alan_id = 0
        else:
            conn.close()
            return jsonify({'hata': 'Geçersiz hareket tipi'}), 400
    else:
        conn.close()
        return jsonify({'hata': 'Yetkisiz kullanıcı'}), 403
    
    mevcut = stok_miktari_getir(gonderen_tip, gonderen_id, palet_tipi_id)
    if mevcut < miktar:
        conn.close()
        return jsonify({'hata': f'Yetersiz stok! Mevcut: {mevcut}'}), 400
    
    basarili, hata = stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, -miktar)
    if not basarili:
        conn.close()
        return jsonify({'hata': hata}), 400
    
    basarili, hata = stok_guncelle(alan_tip, alan_id, palet_tipi_id, +miktar)
    if not basarili:
        stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, +miktar)
        conn.close()
        return jsonify({'hata': hata}), 400
    
    aciklama = f"{palet[1]} - {miktar} adet transfer"
    hareket_kaydet(kullanici_id, hareket_tipi, gonderen_tip, gonderen_id,
                   alan_tip, alan_id, palet_tipi_id, miktar, aciklama)
    
    conn.close()
    return jsonify({'success': True, 'mesaj': 'Transfer başarılı'})


@app.route('/api/dagitici_listesi', methods=['GET'])
@token_required
def get_dagitici_listesi(current_user):
    """Tüm dağıtıcıları listele (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, kullanici_adi, ad_soyad FROM kullanicilar
        WHERE tip = 'DAGITICI'
        ORDER BY ad_soyad
    ''')
    sonuc = cursor.fetchall()
    conn.close()
    
    dagiticilar = [{'id': d[0], 'kullanici_adi': d[1], 'ad_soyad': d[2]} for d in sonuc]
    return jsonify(dagiticilar)


# ==================== HAREKETLER ====================

@app.route('/api/hareketler', methods=['GET'])
@token_required
def get_hareketler(current_user):
    """Hareket geçmişi"""
    limit = request.args.get('limit', 50, type=int)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    if current_user['tip'] == 'DEPOCU':
        cursor.execute('''
            SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi,
                   pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
            FROM hareketler h
            JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
            JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
            ORDER BY h.tarih DESC
            LIMIT ?
        ''', (limit,))
    else:
        cursor.execute('''
            SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi,
                   pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
            FROM hareketler h
            JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
            JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
            WHERE h.gonderen_id = ? OR h.alan_id = ?
            ORDER BY h.tarih DESC
            LIMIT ?
        ''', (current_user['id'], current_user['id'], limit))
    
    sonuc = cursor.fetchall()
    conn.close()
    
    hareketler = []
    for h in sonuc:
        tip_text = {
            'DEPO_DAGITICI': 'Depo→Dağıtıcı',
            'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri',
            'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı',
            'DAGITICI_DEPO': 'Dağıtıcı→Depo'
        }.get(h[3], h[3])
        
        hareketler.append({
            'tarih': h[0],
            'yapan': f"{h[2]} ({h[1]})",
            'islem_tipi': tip_text,
            'stok_kodu': h[4],
            'palet_adi': h[5],
            'miktar': h[6],
            'aciklama': h[7]
        })
    
    return jsonify(hareketler)


# ==================== EXCEL YÜKLEME ====================

@app.route('/api/musteri_excel_yukle', methods=['POST'])
@token_required
def musteri_excel_yukle(current_user):
    """Excel'den müşteri yükle (sadece depocu)"""
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    if 'file' not in request.files:
        return jsonify({'hata': 'Dosya bulunamadı'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'hata': 'Dosya seçilmedi'}), 400
    
    try:
        df = pd.read_excel(file)
    except Exception as e:
        return jsonify({'hata': f'Excel okunamadı: {str(e)}'}), 400
    
    required_columns = ['MÜŞTERİ KODU', 'MÜŞTERİ ADI', 'TABELA ADI']
    for col in required_columns:
        if col not in df.columns:
            return jsonify({'hata': f"'{col}' sütunu bulunamadı. Sütunlar: {list(df.columns)}"}), 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('SELECT id, kullanici_adi FROM kullanicilar WHERE tip = "DAGITICI"')
    dagiticilar = {d[1]: d[0] for d in cursor.fetchall()}
    
    eklenen = 0
    hatalar = []
    
    for index, row in df.iterrows():
        musteri_kodu = str(row['MÜŞTERİ KODU']).strip()
        musteri_adi = str(row['MÜŞTERİ ADI']).strip()
        tabela_adi = str(row['TABELA ADI']).strip()
        
        if not musteri_kodu or not musteri_adi or not tabela_adi:
            hatalar.append(f"Satır {index+2}: Boş alan var")
            continue
        
        if tabela_adi not in dagiticilar:
            hatalar.append(f"Satır {index+2}: '{tabela_adi}' adlı dağıtıcı bulunamadı")
            continue
        
        bagli_dagitici_id = dagiticilar[tabela_adi]
        
        try:
            cursor.execute('''
                INSERT INTO musteriler (musteri_kodu, musteri_adi, bagli_dagitici_id)
                VALUES (?, ?, ?)
            ''', (musteri_kodu, musteri_adi, bagli_dagitici_id))
            
            musteri_id = cursor.lastrowid
            
            cursor.execute("SELECT id FROM palet_tipleri")
            paletler = cursor.fetchall()
            for palet in paletler:
                cursor.execute('''
                    INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                    VALUES (?, ?, ?, 0)
                ''', (SAHIP_TIP_MUSTERI, musteri_id, palet[0]))
            
            eklenen += 1
            
        except sqlite3.IntegrityError:
            hatalar.append(f"Satır {index+2}: '{musteri_kodu}' kodu zaten kayıtlı")
            continue
    
    conn.commit()
    conn.close()
    
    return jsonify({
        'success': True,
        'eklenen': eklenen,
        'hatalar': hatalar,
        'mesaj': f'{eklenen} müşteri eklendi. {len(hatalar)} hata oluştu.'
    })


if __name__ == '__main__':
    veritabani_olustur()
    app.run(host='0.0.0.0', port=5000, debug=False)
